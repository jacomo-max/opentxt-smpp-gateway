// Per-bind status codes for customers whose billing reads SMPP codes (Lexico, Nov 5 2026).
//
// SMPP_STATUS_FALLBACKS  "otxt01:0x1fff"  instant local refusals (DNC/STOP/spam-pause/
//   dropped link) use the same code the backend already returns for this bind
//   (api_keys.smpp_vendor_status_fallback). Our vendor-range codes 0x451-0x457 are
//   unknown to these clients ("VENDOR_SPECIFIC_ERROR").
// SMPP_REJECT_CODES      "otxt01:0x45"    uncharged REJECTD receipts carry this code in
//   the receipt text (err:045, hex) and in the network_error_code TLV, and uncharged
//   "failed" receipts go out as REJECTD instead of UNDELIV. Charged failures unchanged.
const parse = (raw) => {
  const out = new Map();
  for (const part of String(raw || '').split(',')) {
    const [sys, code] = part.split(':').map((s) => s && s.trim());
    const n = Number(code);
    if (sys && Number.isInteger(n) && n > 0 && n <= 0xffff) out.set(sys.toLowerCase(), n);
  }
  return out;
};

const fallbacks = parse(process.env.SMPP_STATUS_FALLBACKS ?? 'otxt01:0x1fff');
const rejects = parse(process.env.SMPP_REJECT_CODES ?? 'otxt01:0x45');

const isVendor = (s) => Number.isInteger(s) && s >= 0x400 && s <= 0x4ff;

/** submit_sm_resp status for a local refusal on this bind. */
export function refusalStatusFor(systemId, status, map = fallbacks) {
  const fb = map.get(String(systemId || '').toLowerCase());
  return fb && isVendor(status) ? fb : status;
}

/** Code for uncharged REJECTD receipts on this bind, or null (legacy err:001, no TLV). */
export function rejectCodeFor(systemId, map = rejects) {
  return map.get(String(systemId || '').toLowerCase()) ?? null;
}

/** Receipt text err field: 3 hex digits (045), wider codes as-is (1fff). */
export const errField = (code) => code.toString(16).toUpperCase().padStart(3, '0');

/** network_error_code TLV: network type 3 (GSM) + 2-byte error code. */
export const networkErrorTlv = (code) => Buffer.from([0x03, (code >> 8) & 0xff, code & 0xff]);

export const _parse = parse;
