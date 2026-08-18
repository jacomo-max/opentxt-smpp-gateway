import { config } from './config.js';

// Sends one message through the OpenTxt HTTP API.
// Returns { ok, id, error }.
export async function sendSms({ apiKey, to, message, idempotencyKey }) {
  const url = `${config.apiBaseUrl}/api-send-sms`;
  const headers = {
    Authorization: `Bearer ${apiKey}`,
    'Content-Type': 'application/json',
  };
  if (idempotencyKey) headers['Idempotency-Key'] = idempotencyKey;

  let res;
  try {
    res = await fetch(url, {
      method: 'POST',
      headers,
      body: JSON.stringify({ to, message }),
    });
  } catch (e) {
    return { ok: false, error: { code: 'network_error', message: e.message } };
  }

  let body = {};
  try {
    body = await res.json();
  } catch {
    /* non-JSON error page */
  }

  if (!res.ok) {
    return {
      ok: false,
      status: res.status,
      error: body?.error || { code: 'http_error', message: `HTTP ${res.status}` },
    };
  }
  return { ok: true, id: body?.id || body?.data?.id, raw: body };
}
