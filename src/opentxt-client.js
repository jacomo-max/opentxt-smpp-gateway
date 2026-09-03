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

// ---------------------------------------------------------------------------
// Durable message-id correlation map
// ---------------------------------------------------------------------------
// The in-memory messageIndex is wiped on every restart/redeploy, which orphans
// any message still awaiting a delivery receipt. These helpers mirror the map
// into OpenTxt so a restart no longer destroys correlation.

/** Fire-and-forget persist of one or more mappings. Never throws. */
export async function storeMessageMap({ apiKey, systemId, entries }) {
  if (!Array.isArray(entries) || entries.length === 0) return { ok: false };
  try {
    const res = await fetch(`${config.apiBaseUrl}/smpp-message-map`, {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${apiKey}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ system_id: systemId, entries }),
    });
    if (!res.ok) {
      const text = await res.text().catch(() => '');
      return { ok: false, status: res.status, error: text.slice(0, 200) };
    }
    return { ok: true };
  } catch (e) {
    return { ok: false, error: e?.message };
  }
}

/**
 * Look a mapping back up after a restart.
 * Accepts one of { requestId, smppMessageId, supplierMessageId }.
 * Returns the stored row or null.
 */
export async function lookupMessageMap({ apiKey, requestId, smppMessageId, supplierMessageId }) {
  const params = new URLSearchParams();
  if (requestId) params.set('request_id', requestId);
  else if (smppMessageId) params.set('smpp_message_id', smppMessageId);
  else if (supplierMessageId) params.set('supplier_message_id', supplierMessageId);
  else return null;

  try {
    const res = await fetch(`${config.apiBaseUrl}/smpp-message-map?${params}`, {
      headers: { Authorization: `Bearer ${apiKey}` },
    });
    if (!res.ok) return null;
    const body = await res.json().catch(() => null);
    if (!body?.found) return null;
    return body;
  } catch {
    return null;
  }
}
