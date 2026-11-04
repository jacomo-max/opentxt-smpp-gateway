// End-to-end test of the hybrid queue with a real SMPP client and a mock backend.
// Run: node test/hybrid-queue.mjs
import http from 'node:http';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { spawn } from 'node:child_process';
import assert from 'node:assert/strict';
import smpp from 'smpp';

const API_PORT = 18182, SMPP_PORT = 12875, HTTP_PORT = 18183;
const state = {
  dnc: ['+15550000001', '+15550000002'],
  opt: ['+15550000003'],
  sup: [{ phone: '+15550000004', blocked_until: new Date(Date.now() + 86400e3).toISOString() }],
  drop: ['bad.my'],
  feedDown: false,
  latencyMs: 900,
  failFirst: new Map(), // idemKey -> remaining 503s
  sends: [], // {key, to, message, at, headers}
  accepted: new Map(), // idemKey -> id
  hold: false,
};
const delta = { dnc: [], opt: [] };
const json = (res, code, body) => { res.writeHead(code, { 'Content-Type': 'application/json' }); res.end(JSON.stringify(body)); };
const api = http.createServer(async (req, res) => {
  let body = ''; for await (const c of req) body += c;
  const url = new URL(req.url, 'http://x');
  if (url.pathname === '/smpp-gateway-feed') {
    if (state.feedDown) return json(res, 500, { error: 'down' });
    if (req.headers['x-otxt-gateway-token'] !== 'tok' || !/^Bearer otxt_/.test(req.headers.authorization || '')) return json(res, 403, { error: 'Gateway authorization required' });
    const list = url.searchParams.get('list');
    const after = url.searchParams.get('after') || '';
    const page = (arr) => { const rows = arr.filter((p) => p > after).sort().slice(0, 1).map((phone) => ({ phone })); return { rows, next_after: rows.length ? rows.at(-1).phone : null }; };
    if (list === 'dnc') return json(res, 200, page(state.dnc));
    if (list === 'opt_out') return json(res, 200, page(state.opt));
    if (list === 'drop_domains') return json(res, 200, { rows: state.drop.map((domain) => ({ domain })) });
    if (list === 'suppressed') return json(res, 200, { rows: after ? [] : state.sup, next_after: null });
    if (list === 'suppressed_delta') return json(res, 200, { rows: [], next_since: new Date().toISOString() });
    if (list === 'delta') { const out = { dnc: delta.dnc.splice(0), opt_out: delta.opt.splice(0), drop_domains: state.drop, next_since: new Date().toISOString(), more: false }; return json(res, 200, out); }
    return json(res, 400, {});
  }
  if (url.pathname === '/smpp-message-map') return json(res, 200, req.method === 'GET' ? { found: false } : { ok: true });
  if (url.pathname === '/api-send-sms') {
    const data = JSON.parse(body); const key = req.headers['idempotency-key'];
    while (state.hold) await new Promise((r) => setTimeout(r, 50));
    state.sends.push({ key, to: data.to, message: data.message, at: Date.now(), headers: req.headers });
    await new Promise((r) => setTimeout(r, state.latencyMs));
    const left = state.failFirst.get(key) || 0;
    if (left > 0) { state.failFirst.set(key, left - 1); return json(res, 503, { error: { code: 'temporarily_unavailable', message: 'busy' } }); }
    if (data.message.includes('refuse')) return json(res, 422, { error: { code: 'compliance_blocked', message: 'Message blocked' } });
    if (!state.accepted.has(key)) state.accepted.set(key, `req-${state.accepted.size + 1}`);
    return json(res, 202, { data: { id: state.accepted.get(key) } });
  }
  json(res, 404, {});
});
await new Promise((r) => api.listen(API_PORT, '127.0.0.1', r));

const qdir = fs.mkdtempSync(path.join(os.tmpdir(), 'smppq-'));
const env = {
  ...process.env,
  SMPP_ACCOUNTS: JSON.stringify([{ system_id: 'OTXT04', password: 'testonly', api_key: 'otxt_k04' }, { system_id: 'OTXT06', password: 'testonly', api_key: 'otxt_k06' }]),
  GATEWAY_WEBHOOK_TOKEN: 'tok', OPENTXT_API_BASE_URL: `http://127.0.0.1:${API_PORT}`, SMPP_PORT: String(SMPP_PORT), PORT: String(HTTP_PORT),
  QUEUE_DIR: qdir, QUEUE_ALLOW_EPHEMERAL: '1', GUARD_DELTA_MS: '300', GUARD_STALE_MS: '1500', FUNCTION_REGION: 'off', FEED_BASE_URL: `http://127.0.0.1:${API_PORT}`, SUBMIT_ACK_MS: '8000',
};
let child, logs = '';
async function startGateway() {
  logs = '';
  child = spawn(process.execPath, ['src/index.js'], { cwd: new URL('..', import.meta.url), env, stdio: ['ignore', 'pipe', 'pipe'] });
  child.stdout.on('data', (x) => (logs += x)); child.stderr.on('data', (x) => (logs += x));
  await waitFor(() => logs.includes(`listening on :${SMPP_PORT}`), 5000, 'gateway start');
}
async function waitFor(fn, ms, what) { const end = Date.now() + ms; while (Date.now() < end) { if (await fn()) return; await new Promise((r) => setTimeout(r, 25)); } throw new Error(`timeout: ${what}\n${logs.slice(-3000)}`); }
const health = async () => (await (await fetch(`http://127.0.0.1:${HTTP_PORT}/health`)).json()).hybrid_queue;
async function bind(systemId) {
  const s = smpp.connect(`smpp://127.0.0.1:${SMPP_PORT}`);
  s.on('error', () => {});
  await new Promise((resolve, reject) => s.bind_transceiver({ system_id: systemId, password: 'testonly' }, (p) => (p.command_status ? reject(Error('bind')) : resolve())));
  s.dlrs = []; s.on('deliver_sm', (p) => { s.dlrs.push(String(p.short_message?.message ?? p.short_message)); s.send(p.response()); });
  return s;
}
const submit = (s, fields) => new Promise((resolve) => { const t = Date.now(); s.submit_sm({ registered_delivery: 1, ...fields }, (r) => resolve({ ...r, ms: Date.now() - t })); });
const pass = (m) => console.log('PASS:', m);
const killer = setTimeout(() => { console.log('TIMEOUT\n', logs.slice(-4000)); child?.kill('SIGKILL'); process.exit(1); }, 180_000); killer.unref();

try {
  await startGateway();
  await waitFor(async () => (await health()).active_for !== undefined && (await health()).recipient_guard.ready, 5000, 'guard ready');
  const lx = await bind('OTXT04');
  await waitFor(async () => (await health()).active_for.includes('otxt04'), 3000, 'otxt04 active');

  // 1. Instant local refusals, nothing sent to the backend.
  const before = state.sends.length;
  const cases = [['15550000001', 'hi', 0x451], ['15550000003', 'hi', 0x452], ['15550000004', 'hi', 0x457], ['15559990000', 'go to https://bad.my/x now', 0x453]];
  for (const [to, msg, code] of cases) {
    const r = await submit(lx, { destination_addr: to, short_message: msg });
    assert.equal(r.command_status, code); assert.ok(r.ms < 200, `refusal took ${r.ms}ms`);
  }
  await new Promise((r) => setTimeout(r, 300));
  assert.equal(state.sends.length, before);
  pass('DNC 0x451, STOP 0x452, spam-paused 0x457, dropped link 0x453: instant, zero backend calls');

  // 2. Normal text acknowledged instantly though the backend takes 900 ms; sent once with the same idempotency key.
  const ok = await submit(lx, { destination_addr: '15551230001', short_message: 'hello queued' });
  assert.equal(ok.command_status, 0); assert.ok(ok.message_id); assert.ok(ok.ms < 200, `ack took ${ok.ms}ms`);
  await waitFor(() => state.accepted.has(`smpp_OTXT04_${ok.message_id}`), 5000, 'queued send');
  const sent = state.sends.find((x) => x.key === `smpp_OTXT04_${ok.message_id}`);
  assert.equal(sent.headers['x-otxt-no-queue'], undefined);
  assert.ok(Math.abs(Number(sent.headers['x-otxt-submit-received-at']) - sent.at) < 1000, 'deadline stamped at dispatch time');
  pass(`accepted in ${ok.ms}ms, sent from queue with idempotency key, backend deadline starts at dispatch`);

  // 2b. Long (multipart) text: parts acknowledged instantly, joined and sent once; DNC parts refused.
  const part = (ref, seq, text, to = '15551230005') => ({ destination_addr: to, short_message: text, sar_msg_ref_num: ref, sar_total_segments: 2, sar_segment_seqnum: seq });
  const mp = await Promise.all([submit(lx, part(7, 2, 'world')), submit(lx, part(7, 1, 'hello '))]);
  assert.ok(mp.every((r) => r.command_status === 0 && r.message_id && r.ms < 200));
  await waitFor(() => state.sends.some((x) => x.message === 'hello world'), 5000, 'multipart send');
  assert.equal(state.sends.filter((x) => x.message === 'hello world').length, 1);
  const mpd = await Promise.all([submit(lx, part(8, 1, 'a', '15550000001')), submit(lx, part(8, 2, 'b', '15550000001'))]);
  assert.ok(mpd.every((r) => r.command_status === 0x451));
  pass('multipart: instant acks, joined text sent once; DNC multipart refused per part');

  // 3. Late refusal becomes a REJECTD receipt.
  const bad = await submit(lx, { destination_addr: '15551230002', short_message: 'please refuse me' });
  assert.equal(bad.command_status, 0);
  await waitFor(() => lx.dlrs.some((d) => d.includes(`id:${bad.message_id}`) && d.includes('stat:REJECTD')), 5000, 'REJECTD');
  pass('backend refusal after acceptance -> REJECTD receipt with the same message id');

  // 4. Temporary backend errors are retried, then sent exactly once.
  state.latencyMs = 50;
  state.hold = true; // keep the first call from reaching the backend until the failure plan is set
  const retryOk = await submit(lx, { destination_addr: '15551230004', short_message: 'retry me too' });
  state.failFirst.set(`smpp_OTXT04_${retryOk.message_id}`, 2);
  state.hold = false;
  await waitFor(() => state.accepted.has(`smpp_OTXT04_${retryOk.message_id}`), 15000, 'retry success');
  assert.ok(!lx.dlrs.some((d) => d.includes(`id:${retryOk.message_id}`)));
  pass('503 twice -> retried with backoff -> accepted, no failure receipt');

  // 5. Daily list changes reach the gateway automatically (delta sync).
  delta.dnc.push('+15550000099');
  await new Promise((r) => setTimeout(r, 900));
  const fresh = await submit(lx, { destination_addr: '15550000099', short_message: 'hi' });
  assert.equal(fresh.command_status, 0x451);
  pass('number added to do-not-contact list is refused within one sync interval');

  // 6. Load: Lexico at 25 TPS while another customer bursts against a slow backend.
  state.latencyMs = 3000;
  const other = await bind('OTXT06');
  const otherBurst = Promise.all(Array.from({ length: 600 }, (_, i) => submit(other, { destination_addr: `1555800${String(i).padStart(4, '0')}`, short_message: `other ${i}` })));
  const lxResults = [];
  const N = 500;
  for (let i = 0; i < N; i++) {
    const dncHit = i % 4 === 0; // ~25% DNC like real Lexico traffic
    lxResults.push(submit(lx, { destination_addr: dncHit ? '15550000002' : `1555700${String(i).padStart(4, '0')}`, short_message: `lexico ${i}` }));
    await new Promise((r) => setTimeout(r, 40));
  }
  const lr = await Promise.all(lxResults);
  const ms = lr.map((r) => r.ms).sort((a, b) => a - b);
  const p95 = ms[Math.floor(ms.length * 0.95)], max = ms.at(-1);
  const accepted = lr.filter((r) => r.command_status === 0).length, refused = lr.filter((r) => r.command_status === 0x451).length;
  assert.equal(accepted + refused, N, 'no throttles or failures');
  assert.ok(p95 < 150, `p95 ${p95}ms`);
  await otherBurst;
  await waitFor(() => lr.filter((r) => r.command_status === 0).every((r) => state.accepted.has(`smpp_OTXT04_${r.message_id}`)), 60000, 'all lexico texts sent');
  pass(`25 TPS for 20s during a 600-text burst from another account on a 3s backend: ${accepted} accepted + ${refused} DNC refused, 0 failures, submit_resp p95 ${p95}ms max ${max}ms; every accepted text sent`);
  other.close();

  // 7. Crash recovery: kill -9 with texts queued, restart, they are sent (once).
  state.latencyMs = 50; state.hold = true;
  const queued = [];
  for (let i = 0; i < 20; i++) queued.push(await submit(lx, { destination_addr: `1555600${String(i).padStart(4, '0')}`, short_message: `crash ${i}` }));
  assert.ok(queued.every((r) => r.command_status === 0));
  child.kill('SIGKILL'); await new Promise((r) => child.once('exit', r));
  state.hold = false;
  await startGateway();
  await waitFor(() => queued.every((r) => state.accepted.has(`smpp_OTXT04_${r.message_id}`)), 10000, 'replay after crash');
  pass('gateway killed with 20 accepted texts in the queue -> all 20 replayed and sent after restart (same idempotency keys)');

  // 8. Feed outage -> falls back to strict (wait for the backend answer).
  await waitFor(async () => (await health()).recipient_guard.ready, 5000, 'guard ready after restart');
  const lx2 = await bind('OTXT04');
  state.feedDown = true; state.latencyMs = 600;
  await waitFor(async () => !(await health()).active_for.includes('otxt04'), 5000, 'fallback');
  const strict = await submit(lx2, { destination_addr: '15551239999', short_message: 'strict path' });
  assert.equal(strict.command_status, 0); assert.ok(strict.ms >= 550, `strict ack ${strict.ms}ms`);
  assert.equal(state.sends.at(-1).headers['x-otxt-no-queue'], '1');
  pass('refusal lists stale -> automatically back to strict wait-for-backend behaviour (no blind accepts)');
  lx.close(); lx2.close();
  console.log('ALL HYBRID QUEUE TESTS PASSED');
} finally {
  clearTimeout(killer); child?.kill('SIGKILL'); api.close();
}
process.exit(0);
