// Local copy of the backend's instant-refusal lists, so queued binds can refuse
// do-not-contact / STOP / spam-paused recipients and carrier-dropped links in
// microseconds without touching the database.
//
// Lists (same ones api-send-sms refuses on):
//   dnc       public.dnc_numbers           (global)      -> 0x451
//   opt_out   public.opt_outs              (global)      -> 0x452
//   suppressed public.carrier_spam_suppressions (per key's user, blocked_until > now) -> 0x457
//   drop_domains active_link_drop_domains() (global)     -> 0x453
//
// Sync: full download on start and every FULL_REFRESH_MS, deltas every
// DELTA_MS (created_at / last_hit_at cursor with overlap). If the feed goes
// stale the guard reports not-ready and the gateway falls back to the
// original strict behaviour (wait for the backend's own answer).
// The backend still checks every text, so a number added seconds ago is
// refused there and reported as a REJECTD receipt; nothing slips through.

export const REFUSAL = {
  dnc: { status: 0x451, reason: 'recipient on global do-not-contact list' },
  opted_out: { status: 0x452, reason: 'recipient opted out (STOP)' },
  suppressed: { status: 0x457, reason: 'recipient carrier spam-blocked in last 7 days' },
  link_dropped: { status: 0x453, reason: 'carriers are dropping this link' },
};

const toKey = (phone) => {
  const digits = String(phone || '').replace(/\D/g, '');
  if (!digits || digits.length > 15) return null;
  return Number(digits);
};

// Same rule as backend _shared/link-drop-guard.ts linkDomainOf().
export function linkDomainOf(body) {
  const m = String(body || '').toLowerCase().match(/(?:https?:\/\/)?((?:[a-z0-9-]+\.)+[a-z]{2,24})\//);
  return m ? m[1].replace(/^www\./, '') : null;
}

// Sorted Float64Array + small Set for additions since the last full load.
// ~3M numbers = ~24 MB instead of ~250 MB for a Set of strings.
class PhoneSet {
  constructor() {
    this.base = new Float64Array(0);
    this.extra = new Set();
  }
  replace(numbers) {
    const arr = Float64Array.from(numbers);
    arr.sort();
    this.base = arr;
    this.extra = new Set();
  }
  add(n) {
    if (n != null && !this.#inBase(n)) this.extra.add(n);
  }
  has(n) {
    return n != null && (this.extra.has(n) || this.#inBase(n));
  }
  #inBase(n) {
    const a = this.base;
    let lo = 0;
    let hi = a.length - 1;
    while (lo <= hi) {
      const mid = (lo + hi) >>> 1;
      const v = a[mid];
      if (v === n) return true;
      if (v < n) lo = mid + 1;
      else hi = mid - 1;
    }
    return false;
  }
  get size() {
    return this.base.length + this.extra.size;
  }
}

export class RecipientGuard {
  constructor({ apiBaseUrl, headers = {}, fetchImpl = fetch, deltaMs = 60_000, fullRefreshMs = 24 * 3600_000, staleMs = 15 * 60_000, overlapMs = 5 * 60_000, pageSize = 50_000, log = () => {} }) {
    Object.assign(this, { apiBaseUrl, headers, fetchImpl, deltaMs, fullRefreshMs, staleMs, overlapMs, pageSize, log });
    this.dnc = new PhoneSet();
    this.optOut = new PhoneSet();
    this.suppressed = new Map(); // systemId -> Map(phoneKey -> blockedUntilMs)
    this.dropDomains = new Set();
    this.accounts = new Map(); // systemId -> apiKey
    this.lastFullAt = 0;
    this.lastDeltaAt = 0;
    this.cursor = null; // ISO time for global lists
    this.supCursor = new Map(); // systemId -> ISO
    this.loadedSystems = new Set();
    this.lastError = null;
    this.timer = null;
    this.running = false;
  }

  addAccount(systemId, apiKey) {
    this.accounts.set(systemId, apiKey);
  }

  /** Usable for a given bind only when the global lists and that bind's suppressions are fresh. */
  ready(systemId) {
    const now = Date.now();
    return this.lastFullAt > 0 && now - this.lastDeltaAt < this.staleMs && this.loadedSystems.has(systemId);
  }

  check(systemId, phone, message) {
    const k = toKey(phone);
    if (this.dnc.has(k)) return 'dnc';
    if (this.optOut.has(k)) return 'opted_out';
    const until = this.suppressed.get(systemId)?.get(k);
    if (until && until > Date.now()) return 'suppressed';
    const d = linkDomainOf(message);
    if (d && this.dropDomains.has(d)) return 'link_dropped';
    return null;
  }

  stats() {
    return {
      ready: this.lastFullAt > 0 && Date.now() - this.lastDeltaAt < this.staleMs,
      dnc: this.dnc.size,
      opt_out: this.optOut.size,
      suppressed: Object.fromEntries([...this.suppressed].map(([s, m]) => [s, m.size])),
      drop_domains: this.dropDomains.size,
      last_full_at: this.lastFullAt ? new Date(this.lastFullAt).toISOString() : null,
      last_delta_at: this.lastDeltaAt ? new Date(this.lastDeltaAt).toISOString() : null,
      last_error: this.lastError,
    };
  }

  async #get(apiKey, params) {
    const url = `${this.apiBaseUrl}/smpp-gateway-feed?${new URLSearchParams(params)}`;
    const res = await this.fetchImpl(url, {
      headers: { Authorization: `Bearer ${apiKey}`, ...this.headers },
      signal: AbortSignal.timeout(60_000),
    });
    if (!res.ok) throw new Error(`feed ${params.list || 'delta'} HTTP ${res.status}`);
    return res.json();
  }

  async #pageAll(apiKey, list, onRow) {
    let after = '';
    for (let page = 0; page < 10_000; page++) {
      const body = await this.#get(apiKey, { list, after, limit: String(this.pageSize) });
      for (const row of body.rows || []) onRow(row);
      if (!body.next_after) return;
      after = body.next_after;
    }
    throw new Error(`feed ${list} paging did not finish`);
  }

  firstKey() {
    return this.accounts.values().next().value;
  }

  async fullRefresh() {
    const key = this.firstKey();
    if (!key) return;
    const startedAt = new Date(Date.now() - this.overlapMs).toISOString();
    const dnc = [];
    const opt = [];
    await this.#pageAll(key, 'dnc', (r) => { const k = toKey(r.phone ?? r); if (k != null) dnc.push(k); });
    await this.#pageAll(key, 'opt_out', (r) => { const k = toKey(r.phone ?? r); if (k != null) opt.push(k); });
    const meta = await this.#get(key, { list: 'drop_domains' });
    this.dnc.replace(dnc);
    this.optOut.replace(opt);
    this.dropDomains = new Set((meta.rows || []).map((d) => String(d.domain ?? d).toLowerCase()).filter(Boolean));
    for (const [systemId, apiKey] of this.accounts) await this.#fullSuppressions(systemId, apiKey);
    this.cursor = startedAt;
    this.lastFullAt = Date.now();
    this.lastDeltaAt = Date.now();
    this.lastError = null;
    this.log(`[guard] full refresh: dnc=${this.dnc.size} opt_out=${this.optOut.size} drop_domains=${this.dropDomains.size}`);
  }

  async #fullSuppressions(systemId, apiKey) {
    const startedAt = new Date(Date.now() - this.overlapMs).toISOString();
    const m = new Map();
    const now = Date.now();
    await this.#pageAll(apiKey, 'suppressed', (r) => {
      const until = Date.parse(r.blocked_until);
      const k = toKey(r.phone);
      if (k != null && until > now) m.set(k, until);
    });
    this.suppressed.set(systemId, m);
    this.supCursor.set(systemId, startedAt);
    this.loadedSystems.add(systemId);
  }

  async delta() {
    const key = this.firstKey();
    if (!key || !this.cursor) return;
    // Global lists. Loop while the endpoint reports more rows.
    for (let i = 0; i < 50; i++) {
      const body = await this.#get(key, { list: 'delta', since: this.cursor, limit: String(this.pageSize) });
      for (const p of body.dnc || []) this.dnc.add(toKey(p.phone ?? p));
      for (const p of body.opt_out || []) this.optOut.add(toKey(p.phone ?? p));
      if (Array.isArray(body.drop_domains)) this.dropDomains = new Set(body.drop_domains.map((d) => String(d.domain ?? d).toLowerCase()).filter(Boolean));
      const next = body.next_since ? Date.parse(body.next_since) : NaN;
      if (Number.isFinite(next)) this.cursor = new Date(next - this.overlapMs).toISOString();
      if (!body.more) break;
    }
    for (const [systemId, apiKey] of this.accounts) {
      if (!this.loadedSystems.has(systemId)) { await this.#fullSuppressions(systemId, apiKey); continue; }
      const body = await this.#get(apiKey, { list: 'suppressed_delta', since: this.supCursor.get(systemId), limit: String(this.pageSize) });
      const m = this.suppressed.get(systemId);
      const now = Date.now();
      for (const r of body.rows || []) {
        const k = toKey(r.phone);
        const until = Date.parse(r.blocked_until);
        if (k == null) continue;
        if (until > now) m.set(k, until); else m.delete(k);
      }
      for (const [k, until] of m) if (until <= now) m.delete(k);
      const next = body.next_since ? Date.parse(body.next_since) : NaN;
      if (Number.isFinite(next)) this.supCursor.set(systemId, new Date(next - this.overlapMs).toISOString());
    }
    this.lastDeltaAt = Date.now();
    this.lastError = null;
  }

  async tick() {
    if (this.running) return;
    this.running = true;
    try {
      if (!this.lastFullAt || Date.now() - this.lastFullAt > this.fullRefreshMs) await this.fullRefresh();
      else await this.delta();
    } catch (e) {
      this.lastError = `${new Date().toISOString()} ${e?.message || e}`;
      this.log(`[guard] sync failed: ${e?.message || e}`);
    } finally {
      this.running = false;
    }
  }

  start() {
    if (this.timer || this.accounts.size === 0) return;
    this.tick();
    this.timer = setInterval(() => this.tick(), this.deltaMs);
    this.timer.unref?.();
  }

  stop() {
    clearInterval(this.timer);
    this.timer = null;
  }
}
