import http from 'node:http';
import crypto from 'node:crypto';
import smpp from 'smpp';
import { config } from './config.js';
import { RateLimiter } from './rate-limiter.js';
import { sendSms } from './opentxt-client.js';

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

// ---------------------------------------------------------------------------
// Helpers: SMPP payload decoding
// ---------------------------------------------------------------------------
function decodeShortMessage(pdu) {
  // node-smpp gives us either a string (already decoded) or a Buffer.
  const sm = pdu.short_message;
  if (sm == null) return '';
  if (typeof sm === 'string') return sm;
  if (typeof sm === 'object' && typeof sm.message === 'string') return sm.message;
  if (Buffer.isBuffer(sm)) {
    // data_coding 8 == UCS2
    return pdu.data_coding === 8 ? sm.toString('ucs2') : sm.toString('latin1');
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
    session.otxtCanReceive = mode !== 'tx'; // rx + trx get deliver_sm
    session.otxtCanSubmit = mode !== 'rx'; // tx + trx may submit
    const state = getState(systemId, account);
    state.sessions.add(session);
    log(`[smpp] ${systemId} bound (${mode}) @ ${account.tps} tps — ${state.sessions.size} session(s)`);
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
    const message = decodeShortMessage(pdu);
    if (!to || !message) {
      return session.send(pdu.response({ command_status: smpp.ESME_RINVDSTADR }));
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
        to,
        registeredDelivery: Number(pdu.registered_delivery || 0),
        sourceAddr: String(pdu.source_addr || ''),
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
  const targets = receiveSessions(entry.systemId);
  if (targets.length === 0) {
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

  for (const session of targets) {
    session.deliver_sm(
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
  log(`[webhook] DLR ${stateText} -> ${entry.systemId} on ${targets.length} session(s)`);
  return true;
}

function sendMoMessage(systemId, from, to, message) {
  const targets = receiveSessions(systemId);
  if (targets.length === 0) return false;
  for (const session of targets) {
    session.deliver_sm(
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
      }),
    );
  }

  if (req.method === 'POST' && url.pathname === `/webhook/${config.webhookToken}`) {
    let raw = '';
    req.on('data', (c) => {
      raw += c;
      if (raw.length > 1_000_000) req.destroy();
    });
    req.on('end', () => {
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
          const entry = payload.id && messageIndex.get(payload.id);
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
  server.close();
  httpServer.close(() => process.exit(0));
});
