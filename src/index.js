import http from 'node:http';
import crypto from 'node:crypto';
import smpp from 'smpp';
import { config } from './config.js';
import { RateLimiter } from './rate-limiter.js';
import { sendSms, storeMessageMap, lookupMessageMap } from './opentxt-client.js';

const log = (...a) => console.log(new Date().toISOString(), ...a);

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------
// systemId -> { account, limiter, inFlight, sessions: Set<session> }
// One ESME may hold several sessions at once (e.g. a transmitter for submits
// plus a separate receiver worker for delivery receipts), so we keep every
// live session per system_id instead of a single slot.
const binds = new Map();
// opentxt message id -> { systemId, smppMessageId, to, from }
const messageIndex = new Map();
const MESSAGE_TTL_MS = 1000 * 60 * 60 * 24; // keep 24h so late DLRs still map

function rememberMessage(openTxtId, entry) {
  messageIndex.set(openTxtId, { ...entry, at: Date.now() });
  queueDurableMap(openTxtId, entry);
}

// ---------------------------------------------------------------------------
// Durable correlation map (survives restarts)
// ---------------------------------------------------------------------------
// messageIndex above lives in RAM, so a redeploy orphans every message still
// awaiting a receipt. We mirror each mapping into OpenTxt, batched so a 250/s
// submit rate doesn't turn into 250 extra HTTP calls per second.
const MAP_FLUSH_MS = 250;
const MAP_FLUSH_MAX = 200;
const mapQueues = new Map(); // systemId -> { apiKey, rows: [], timer }

function queueDurableMap(openTxtId, entry) {
  if (!entry?.systemId || !entry?.apiKey) return;
  let q = mapQueues.get(entry.systemId);
  if (!q) {
    q = { apiKey: entry.apiKey, rows: [], timer: null, flushing: false };
    mapQueues.set(entry.systemId, q);
  }
  q.apiKey = entry.apiKey;
  q.rows.push({
    smpp_message_id: entry.smppMessageId,
    supplier_message_id: entry.supplierMessageId || null,
    request_id: entry.requestId || openTxtId,
    to_phone: entry.to,
    source_addr: entry.sourceAddr || null,
    submit_session_id: entry.submitSessionId || null,
    registered_delivery: entry.registeredDelivery ?? 0,
  });
  if (q.rows.length >= MAP_FLUSH_MAX) return flushDurableMap(entry.systemId);
  if (!q.timer) {
    q.timer = setTimeout(() => flushDurableMap(entry.systemId), MAP_FLUSH_MS);
    q.timer.unref?.();
  }
}

async function flushDurableMap(systemId) {
  const q = mapQueues.get(systemId);
  if (!q || q.flushing) return;
  if (q.timer) {
    clearTimeout(q.timer);
    q.timer = null;
  }
  const entries = q.rows.splice(0, MAP_FLUSH_MAX);
  if (entries.length === 0) return;
  q.flushing = true;
  const res = await storeMessageMap({ apiKey: q.apiKey, systemId, entries });
  q.flushing = false;
  if (!res.ok) {
    // Never discard correlation rows: without them a restart permanently loses
    // the DLR. Put this batch back ahead of newer rows and retry shortly.
    q.rows.unshift(...entries);
    log(`[map] persist failed for ${systemId}: ${res.status || ''} ${res.error || ''}; retrying`);
    q.timer = setTimeout(() => flushDurableMap(systemId), 1000);
    q.timer.unref?.();
    return;
  }
  if (q.rows.length > 0) flushDurableMap(systemId);
}

/**
 * Resolve a webhook event to a bind entry. Falls back to the durable map when
 * the in-memory index has been wiped by a restart.
 */
// Receipts for messages submitted before the durable map existed (or older than
// its retention) will never resolve. Remember those misses briefly so a backlog
// of unresolvable receipts can't turn into a lookup call per bind per receipt.
const MISS_TTL_MS = 10 * 60 * 1000;
const missCache = new Map(); // openTxtId -> timestamp
setInterval(() => {
  const cutoff = Date.now() - MISS_TTL_MS;
  for (const [k, t] of missCache) if (t < cutoff) missCache.delete(k);
}, 60_000).unref();

async function resolveEntry(openTxtId) {
  if (!openTxtId) return null;
  const known = messageIndex.get(openTxtId);
  if (known) return known;
  const missedAt = missCache.get(openTxtId);
  if (missedAt && Date.now() - missedAt < MISS_TTL_MS) return null;
  // We don't know which account owns the message, so ask each bound account.
  // Binds are few (one per customer), and each key can only read its own rows.
  for (const [systemId, state] of binds) {
    const row = await lookupMessageMap({ apiKey: state.account.apiKey, requestId: openTxtId });
    if (!row) continue;
    const entry = {
      systemId,
      smppMessageId: row.smpp_message_id,
      to: row.to_phone,
      sourceAddr: row.source_addr || '',
      submitSessionId: row.submit_session_id || null,
      registeredDelivery: Number(row.registered_delivery || 0),
      apiKey: state.account.apiKey,
      at: Date.now(),
    };
    messageIndex.set(openTxtId, entry);
    log(`[map] recovered mapping for ${openTxtId} from durable store (${systemId})`);
    return entry;
  }
  missCache.set(openTxtId, Date.now());
  return null;
}

setInterval(() => {
  const cutoff = Date.now() - MESSAGE_TTL_MS;
  for (const [k, v] of messageIndex) if (v.at < cutoff) messageIndex.delete(k);
}, 60_000).unref();

// SMPP message_id must be a short opaque string; we hand the customer a hex id
// and keep the mapping to the OpenTxt UUID in memory.
function shortId() {
  return crypto.randomBytes(8).toString('hex').toUpperCase();
}

function getState(systemId, account) {
  let state = binds.get(systemId);
  if (!state) {
    state = {
      account,
      limiter: new RateLimiter(account.tps),
      inFlight: 0,
      sessions: new Set(),
    };
    binds.set(systemId, state);
  }
  return state;
}

/** Sessions that are allowed to receive deliver_sm (rx + trx binds). */
function receiveSessions(systemId) {
  const state = binds.get(systemId);
  if (!state) return [];
  return [...state.sessions].filter((s) => s.otxtCanReceive);
}

/**
 * Pick exactly ONE session to hand a deliver_sm to.
 *
 * An ESME that holds several trx sessions for throughput must still receive a
 * single receipt per message — fanning the same DLR out to every open session
 * made customers see 2-3 duplicate receipts for one submit and broke their
 * reconciliation. Prefer the session that submitted the message (still bound
 * and receive-capable), otherwise round-robin so load spreads evenly.
 */
function pickReceiveSession(systemId, preferred, preferredSessionId) {
  const targets = receiveSessions(systemId);
  if (targets.length === 0) return null;
  if (preferred && targets.includes(preferred)) return preferred;
  if (preferredSessionId) {
    const restored = targets.find((target) => target.otxtSessionId === preferredSessionId);
    if (restored) return restored;
  }
  const state = binds.get(systemId);
  state.rrCursor = ((state.rrCursor ?? 0) + 1) % targets.length;
  return targets[state.rrCursor];
}

// ---------------------------------------------------------------------------
// Helpers: SMPP payload decoding
// ---------------------------------------------------------------------------
const DATA_CODING_LABELS = new Map([
  [0, 'smsc-default/gsm7'],
  [1, 'ia5/ascii'],
  [3, 'latin1'],
  [8, 'ucs2'],
]);

function dataCodingLabel(raw) {
  const dataCoding = Number(raw || 0);
  return DATA_CODING_LABELS.get(dataCoding) || `vendor-specific-${dataCoding}`;
}

function decodeUcs2Be(buf) {
  let text = '';
  for (let i = 0; i + 1 < buf.length; i += 2) {
    text += String.fromCharCode(buf.readUInt16BE(i));
  }
  return text;
}

function stripUdhIfPresent(buf, pdu) {
  const hasUdh = (Number(pdu.esm_class || 0) & 0x40) === 0x40;
  if (!hasUdh || buf.length === 0) return buf;
  const udhLength = Number(buf[0]);
  if (!Number.isFinite(udhLength) || udhLength < 0 || udhLength + 1 > buf.length) return buf;
  return buf.subarray(udhLength + 1);
}

function shortMessageBuffer(pdu) {
  const sm = pdu.short_message;
  if (sm == null) return null;
  if (Buffer.isBuffer(sm)) return sm;
  if (typeof sm === 'object') {
    if (Buffer.isBuffer(sm.message)) return sm.message;
    if (typeof sm.message === 'string') return Buffer.from(sm.message, 'utf8');
  }
  return null;
}

function decodeShortMessage(pdu) {
  // node-smpp gives us either a string (already decoded), an object, or a Buffer.
  const sm = pdu.short_message;
  if (sm == null) return '';
  if (typeof sm === 'string') return sm;
  if (typeof sm === 'object' && typeof sm.message === 'string') return sm.message;

  const rawBuffer = shortMessageBuffer(pdu);
  if (rawBuffer) {
    const payload = stripUdhIfPresent(rawBuffer, pdu);
    const dataCoding = Number(pdu.data_coding || 0);
    if (dataCoding === 8) return decodeUcs2Be(payload);
    // Accept common SMPP text codings plus provider-specific text defaults.
    // Previously customers using Latin-1/vendor DCS could see confusing data-coding errors.
    return payload.toString(dataCoding === 1 ? 'ascii' : 'latin1');
  }
  return String(sm);
}

function e164(raw) {
  const digits = String(raw || '').replace(/\D/g, '');
  if (!digits) return null;
  if (digits.length === 10) return `+1${digits}`;
  if (digits.length === 11 && digits.startsWith('1')) return `+${digits}`;
  return `+${digits}`;
}

// ---------------------------------------------------------------------------
// SMPP server
// ---------------------------------------------------------------------------
const server = smpp.createServer({ debug: config.logLevel === 'debug' }, (session) => {
  let bound = null;

  session.on('error', (err) => log('[smpp] session error', err?.message));

  session.on('close', () => {
    if (!bound) return;
    const state = binds.get(bound.systemId);
    if (!state) return;
    state.sessions.delete(session);
    log(`[smpp] ${bound.systemId} session closed (${state.sessions.size} left)`);
    if (state.sessions.size === 0) binds.delete(bound.systemId);
  });

  function handleBind(pdu, mode) {
    const systemId = String(pdu.system_id || '');
    const account = config.accounts.get(systemId);
    if (!account || account.password !== String(pdu.password || '')) {
      log(`[smpp] bind rejected for "${systemId}"`);
      return session.send(pdu.response({ command_status: smpp.ESME_RBINDFAIL }));
    }
    bound = account;
    session.otxtMode = mode;
    // Give parallel binds a stable account-local slot. Clients normally reconnect
    // their workers in the same order, allowing a persisted submit slot to route
    // a delayed receipt back to that worker after a gateway restart.
    const usedSessionIds = new Set([...getState(systemId, account).sessions].map((s) => s.otxtSessionId));
    let sessionOrdinal = 1;
    while (usedSessionIds.has(`${mode}:${sessionOrdinal}`)) sessionOrdinal += 1;
    session.otxtSessionId = `${mode}:${sessionOrdinal}`;
    session.otxtCanReceive = mode !== 'tx'; // rx + trx get deliver_sm
    session.otxtCanSubmit = mode !== 'rx'; // tx + trx may submit
    const state = getState(systemId, account);
    state.sessions.add(session);
    log(`[smpp] ${systemId} bound (${mode}) @ ${account.tps} tps - ${state.sessions.size} session(s)`);
    session.send(pdu.response({ system_id: 'opentxt' }));
  }

  session.on('bind_transceiver', (pdu) => handleBind(pdu, 'trx'));
  session.on('bind_transmitter', (pdu) => handleBind(pdu, 'tx'));
  session.on('bind_receiver', (pdu) => handleBind(pdu, 'rx'));

  session.on('enquire_link', (pdu) => session.send(pdu.response()));
  session.on('unbind', (pdu) => {
    session.send(pdu.response());
    session.close();
  });

  session.on('submit_sm', async (pdu) => {
    if (!bound || !session.otxtCanSubmit) {
      return session.send(pdu.response({ command_status: smpp.ESME_RINVBNDSTS }));
    }
    const state = binds.get(bound.systemId);
    if (!state) {
      return session.send(pdu.response({ command_status: smpp.ESME_RINVBNDSTS }));
    }
    if (state.inFlight >= config.maxInFlight) {
      // Backpressure: tell the ESME to slow down instead of silently dropping.
      return session.send(pdu.response({ command_status: smpp.ESME_RTHROTTLED }));
    }

    const to = e164(pdu.destination_addr);
    const message = decodeShortMessage(pdu).trim();
    const dataCoding = Number(pdu.data_coding || 0);
    const dataCodingName = dataCodingLabel(dataCoding);
    const messageBytes = shortMessageBuffer(pdu)?.length || Buffer.byteLength(message, 'utf8');
    if (!to || !message) {
      log(
        `[smpp] ${bound.systemId} invalid submit: to=${pdu.destination_addr || ''} ` +
          `data_coding=${dataCoding}(${dataCodingName}) bytes=${messageBytes}`,
      );
      return session.send(pdu.response({ command_status: smpp.ESME_RINVDSTADR }));
    }
    if (!DATA_CODING_LABELS.has(dataCoding)) {
      log(
        `[smpp] ${bound.systemId} accepting non-standard data_coding=${dataCoding}(${dataCodingName}) ` +
          `as latin1 fallback bytes=${messageBytes}`,
      );
    }

    state.inFlight += 1;
    try {
      await state.limiter.take();
      const smppMessageId = shortId();
      const result = await sendSms({
        apiKey: bound.apiKey,
        to,
        message,
        idempotencyKey: `smpp_${bound.systemId}_${smppMessageId}`,
      });

      if (!result.ok) {
        const code = String(result.error?.code || '').toLowerCase();
        const reason = `${code} ${String(result.error?.message || '').toLowerCase()}`;
        // Suppressed / opted-out / DNC destinations are a policy rejection, not
        // a malformed address. Returning ESME_RINVDSTADR (0x0B) here made
        // customers think their number formatting was wrong.
        const suppressed = /dnc|opt.?out|suppress|unsubscrib|blocked|do_not_call/.test(reason);
        const status =
          result.status === 402
            ? smpp.ESME_RTHROTTLED
            : suppressed
              ? smpp.ESME_RSUBMITFAIL
              : result.status === 422
                ? smpp.ESME_RINVDSTADR
                : smpp.ESME_RSUBMITFAIL;
        log(
          `[smpp] ${bound.systemId} submit failed: ${result.error?.code} ${result.error?.message}` +
            (suppressed ? ' (suppressed destination)' : ''),
        );
        return session.send(pdu.response({ command_status: status }));
      }

      rememberMessage(result.id, {
        systemId: bound.systemId,
        smppMessageId,
        supplierMessageId: result.raw?.supplier_message_id || result.raw?.data?.supplier_message_id || null,
        requestId: result.id,
        to,
        registeredDelivery: Number(pdu.registered_delivery || 0),
        sourceAddr: String(pdu.source_addr || ''),
        apiKey: bound.apiKey,
        session,
        submitSessionId: session.otxtSessionId,
      });
      session.send(pdu.response({ message_id: smppMessageId }));
    } catch (e) {
      log('[smpp] submit_sm error', e?.message);
      session.send(pdu.response({ command_status: smpp.ESME_RSUBMITFAIL }));
    } finally {
      state.inFlight -= 1;
    }
  });
});

server.listen(config.smppPort, () => log(`[smpp] listening on :${config.smppPort}`));

// ---------------------------------------------------------------------------
// Delivery receipts + inbound MO, pushed here by the OpenTxt webhook
// ---------------------------------------------------------------------------
function sendDeliveryReceipt(entry, stateText, statusCode) {
  const target = pickReceiveSession(entry.systemId, entry.session, entry.submitSessionId);
  if (!target) {
    log(`[webhook] no receive-capable session bound for ${entry.systemId}`);
    return false;
  }
  const now = new Date();
  const stamp = `${String(now.getUTCFullYear()).slice(2)}${String(now.getUTCMonth() + 1).padStart(2, '0')}${String(
    now.getUTCDate(),
  ).padStart(2, '0')}${String(now.getUTCHours()).padStart(2, '0')}${String(now.getUTCMinutes()).padStart(2, '0')}`;
  const text =
    `id:${entry.smppMessageId} sub:001 dlvrd:${stateText === 'DELIVRD' ? '001' : '000'} ` +
    `submit date:${stamp} done date:${stamp} stat:${stateText} err:${statusCode} text:`;

  {
    target.deliver_sm(
      {
        source_addr: entry.to.replace('+', ''),
        destination_addr: (entry.sourceAddr || '').replace('+', ''),
        esm_class: 4, // delivery receipt
        short_message: text,
        receipted_message_id: entry.smppMessageId,
        message_state: stateText === 'DELIVRD' ? 2 : 5,
      },
      () => {},
    );
  }
  log(`[webhook] DLR ${stateText} -> ${entry.systemId} (1 session)`);
  return true;
}

function sendMoMessage(systemId, from, to, message) {
  // Same rule as receipts: one copy of an inbound message, not one per session.
  const target = pickReceiveSession(systemId, null, null);
  if (!target) return false;
  {
    target.deliver_sm(
      {
        source_addr: String(from || '').replace('+', ''),
        destination_addr: String(to || '').replace('+', ''),
        short_message: message || '',
      },
      () => {},
    );
  }
  return true;
}

function findSystemIdForApiKeyEvent(payload) {
  // Prefer the mapping we stored at submit time.
  const known = payload.id && messageIndex.get(payload.id);
  if (known) return known.systemId;
  // Single-account gateways: fall back to the only bind.
  if (binds.size === 1) return [...binds.keys()][0];
  return null;
}

const httpServer = http.createServer((req, res) => {
  const url = new URL(req.url, 'http://localhost');

  if (req.method === 'GET' && url.pathname === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    return res.end(
      JSON.stringify({
        ok: true,
        smpp_port: config.smppPort,
        bound: [...binds.keys()],
        sessions: Object.fromEntries(
          [...binds.entries()].map(([id, s]) => [id, [...s.sessions].map((x) => x.otxtMode || '?')]),
        ),
        tracked_messages: messageIndex.size,
        durable_map_queue: [...mapQueues.values()].reduce((n, q) => n + q.rows.length, 0),
        accepted_data_codings: [0, 1, 3, 8],
        non_standard_data_coding_fallback: 'latin1',
      }),
    );
  }

  if (req.method === 'POST' && url.pathname === `/webhook/${config.webhookToken}`) {
    let raw = '';
    req.on('data', (c) => {
      raw += c;
      if (raw.length > 1_000_000) req.destroy();
    });
    req.on('end', async () => {
      let payload = {};
      try {
        payload = JSON.parse(raw || '{}');
      } catch {
        res.writeHead(400).end('bad json');
        return;
      }

      const eventType = payload.event_type || 'delivered';
      try {
        if (eventType === 'delivered' || eventType === 'failed') {
          const entry = await resolveEntry(payload.id);
          if (entry) {
            sendDeliveryReceipt(entry, eventType === 'delivered' ? 'DELIVRD' : 'UNDELIV', eventType === 'delivered' ? '000' : '001');
          } else {
            log(`[webhook] no mapping for message ${payload.id} (${eventType})`);
          }
        } else if (eventType === 'inbound_reply' || eventType === 'opt_out') {
          const systemId = findSystemIdForApiKeyEvent(payload);
          if (systemId) {
            sendMoMessage(systemId, payload.from || payload.to_phone, payload.to || payload.from_phone, payload.message || payload.body);
          } else {
            log('[webhook] inbound event with no bound session to route to');
          }
        }
      } catch (e) {
        log('[webhook] handler error', e?.message);
      }

      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: true }));
    });
    return;
  }

  res.writeHead(404).end('not found');
});

httpServer.listen(config.httpPort, () => log(`[http] listening on :${config.httpPort}`));

process.on('SIGTERM', () => {
  log('shutting down');
  // Persist anything still buffered so a redeploy can't orphan it.
  for (const systemId of mapQueues.keys()) flushDurableMap(systemId);
  server.close();
  httpServer.close(() => process.exit(0));
});
