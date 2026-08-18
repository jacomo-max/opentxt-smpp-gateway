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
    });
  }
  return map;
}

export const config = {
  smppPort: Number(process.env.SMPP_PORT || 2775),
  httpPort: Number(process.env.PORT || 8080),
  // Base URL of the OpenTxt HTTP API (single-send endpoint lives under /api-send-sms)
  apiBaseUrl: (process.env.OPENTXT_API_BASE_URL || 'https://api.opentxt.ai').replace(/\/$/, ''),
  // Shared secret the OpenTxt webhook must present to push DLRs / inbound MO into this gateway
  webhookToken: required('GATEWAY_WEBHOOK_TOKEN'),
  accounts: parseAccounts(),
  maxInFlight: Number(process.env.MAX_IN_FLIGHT || 200),
  logLevel: process.env.LOG_LEVEL || 'info',
};
