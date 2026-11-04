// Configuration is 100% env-driven so nothing secret lives in the repo.

function required(name) {
  const v = process.env[name];
  if (!v) {
    console.error(`[config] Missing required env var ${name}`);
    process.exit(1);
  }
  return v;
}

// ACCOUNTS is a JSON array mapping SMPP system_id/password -> OpenTxt API key.
// Example:
// [{"system_id":"mmdsmart","password":"s3cret","api_key":"otxt_live_xxx","tps":17}]
function parseAccounts() {
  const raw = required('SMPP_ACCOUNTS');
  let parsed;
  try {
    parsed = JSON.parse(raw);
  } catch (e) {
    console.error('[config] SMPP_ACCOUNTS is not valid JSON:', e.message);
    process.exit(1);
  }
  if (!Array.isArray(parsed) || parsed.length === 0) {
    console.error('[config] SMPP_ACCOUNTS must be a non-empty JSON array');
    process.exit(1);
  }
  const map = new Map();
  for (const a of parsed) {
    if (!a.system_id || !a.password || !a.api_key) {
      console.error('[config] Each account needs system_id, password, api_key');
      process.exit(1);
    }
    map.set(String(a.system_id), {
      systemId: String(a.system_id),
      password: String(a.password),
      apiKey: String(a.api_key),
      tps: Number(a.tps || process.env.DEFAULT_TPS || 17),
      // Optional per-account cap on submits waiting for the backend (strict binds hold each
      // submit open for the whole backend call, so in-flight = TPS x backend latency).
      maxInFlight: Number(a.max_in_flight) > 0 ? Number(a.max_in_flight) : null,
    });
  }
  return map;
}

export const config = {
  smppPort: Number(process.env.SMPP_PORT || 2775),
  httpPort: Number(process.env.PORT || 8080),
  // Base URL of the OpenTxt HTTP API (single-send endpoint lives under /api-send-sms)
  apiBaseUrl: (process.env.OPENTXT_API_BASE_URL || 'https://api.opentxt.ai').replace(/\/$/, ''),
  // Run the backend edge function next to the database (us-west-2). Without this the
  // edge function runs near the gateway and every one of its ~6 sequential database
  // calls crosses regions (~190ms each). Set FUNCTION_REGION=off to disable.
  functionRegion: (process.env.FUNCTION_REGION || 'us-west-2').trim(),
  // Shared secret the OpenTxt webhook must present to push DLRs / inbound MO into this gateway
  // Trim whitespace/newlines so a stray space pasted into the hosting dashboard
  // can never 404 the DLR callbacks. Extra comma-separated tokens are ignored.
  webhookToken: required('GATEWAY_WEBHOOK_TOKEN').split(',')[0].trim(),
  accounts: parseAccounts(),
  // 200 throttled strict binds at ~40 TPS once backend latency reached ~5s; 600 covers
  // ~100 TPS at 6s. Env MAX_IN_FLIGHT still overrides.
  maxInFlight: Number(process.env.MAX_IN_FLIGHT) > 0 ? Number(process.env.MAX_IN_FLIGHT) : 600,
  // Longest we hold a submit_sm_resp waiting on the backend before acknowledging
  // anyway (customers commonly time out at 30-60s).
  submitAckMs: Number(process.env.SUBMIT_ACK_MS || 8000),
  // Hard cap on one backend send call; retried once with the same idempotency key.
  sendTimeoutMs: Number(process.env.SEND_TIMEOUT_MS || 90000),
  logLevel: process.env.LOG_LEVEL || 'info',
  // Hybrid queue (accept now, send shortly) for binds listed here. Needs a
  // durable directory (Railway volume at /data) and a fresh recipient-guard
  // feed; otherwise those binds keep the strict wait-for-backend behaviour.
  queuedSystems: (process.env.QUEUED_SYSTEMS ?? 'otxt04').toLowerCase().split(',').map((s) => s.trim()).filter((s) => s && s !== 'none'),
  queueDir: process.env.QUEUE_DIR || '/data/smppgw',
  queueAllowEphemeral: process.env.QUEUE_ALLOW_EPHEMERAL === '1',
  queueMaxAgeMs: Number(process.env.QUEUE_MAX_AGE_MS || 30 * 60_000),
  queueMaxPending: Number(process.env.QUEUE_MAX_PENDING || 200_000),
  queueMaxConcurrency: Number(process.env.QUEUE_MAX_CONCURRENCY || 150),
  // The refusal-list feed is a Supabase function reached directly (api.opentxt.ai
  // serves the web app for GET requests).
  feedBaseUrl: (process.env.FEED_BASE_URL || 'https://ygouapgxpzeztipvpidn.supabase.co/functions/v1').replace(/\/$/, ''),
  guardDeltaMs: Number(process.env.GUARD_DELTA_MS || 60_000),
  guardStaleMs: Number(process.env.GUARD_STALE_MS || 15 * 60_000),
  guardFullRefreshMs: Number(process.env.GUARD_FULL_REFRESH_MS || 24 * 3600_000),
};
