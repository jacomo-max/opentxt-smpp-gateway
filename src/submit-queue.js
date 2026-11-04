// Durable accept-now / send-shortly queue for "queued" binds.
//
// A text is written to an append-only journal before its submit_sm_resp goes
// out, then handed to the backend by workers. Each account gets its own FIFO
// and concurrency; workers take turns between accounts so one customer's burst
// cannot starve another. Concurrency adapts to backend latency (AIMD): it backs
// off when the backend slows down instead of piling more calls onto it.
// On restart every journalled text without a "done" record is replayed. The
// backend dedupes on the same Idempotency-Key, so a replay is never sent or
// charged twice.

import fs from 'node:fs';
import path from 'node:path';

export class SubmitQueue {
  constructor({ dir, handler, log = () => {}, maxConcurrency = 150, minConcurrency = 8, startConcurrency = 60, slowMs = 8000, fastMs = 2500, maxPending = 200_000 }) {
    Object.assign(this, { dir, handler, log, maxConcurrency, minConcurrency, slowMs, fastMs, maxPending });
    this.startConcurrency = startConcurrency;
    this.lanes = new Map(); // systemId -> { items: [], active, limit, done, failed, lastMs: [] }
    this.pending = new Map(); // id -> item
    this.rr = [];
    this.rrIndex = 0;
    this.doneSinceCompact = 0;
    this.journalPath = dir ? path.join(dir, 'submit-queue.jsonl') : null;
    this.fd = null;
    this.timer = null;
    this.paused = false;
  }

  open() {
    if (!this.journalPath) return 0;
    fs.mkdirSync(this.dir, { recursive: true });
    const recovered = new Map();
    if (fs.existsSync(this.journalPath)) {
      const text = fs.readFileSync(this.journalPath, 'utf8');
      for (const line of text.split('\n')) {
        if (!line) continue;
        let rec;
        try { rec = JSON.parse(line); } catch { continue; } // torn last line after a crash
        if (rec.op === 'e' && rec.item?.id) recovered.set(rec.item.id, rec.item);
        else if (rec.op === 'd') recovered.delete(rec.id);
      }
    }
    this.#rewrite([...recovered.values()]);
    for (const item of recovered.values()) this.#push({ ...item, nextAt: 0 });
    if (recovered.size) this.log(`[queue] recovered ${recovered.size} accepted text(s) from journal; replaying`);
    this.kick();
    return recovered.size;
  }

  #rewrite(items) {
    const tmp = `${this.journalPath}.tmp`;
    fs.writeFileSync(tmp, items.map((item) => JSON.stringify({ op: 'e', item })).join('\n') + (items.length ? '\n' : ''));
    fs.renameSync(tmp, this.journalPath);
    if (this.fd != null) fs.closeSync(this.fd);
    this.fd = fs.openSync(this.journalPath, 'a');
    this.doneSinceCompact = 0;
  }

  #append(rec) {
    if (this.fd == null) return;
    fs.writeSync(this.fd, JSON.stringify(rec) + '\n');
  }

  lane(systemId) {
    let l = this.lanes.get(systemId);
    if (!l) {
      l = { items: [], active: 0, limit: this.startConcurrency, done: 0, retried: 0, lastMs: [] };
      this.lanes.set(systemId, l);
      this.rr.push(systemId);
    }
    return l;
  }

  get size() {
    return this.pending.size;
  }

  canAccept() {
    return !this.paused && this.pending.size < this.maxPending;
  }

  /** Journal first (throws if the disk write fails), then schedule. */
  enqueue(item) {
    const rec = { ...item, attempts: 0, nextAt: 0 };
    this.#append({ op: 'e', item: rec });
    this.#push(rec);
    this.kick();
  }

  #push(item) {
    this.pending.set(item.id, item);
    this.lane(item.systemId).items.push(item);
  }

  #finish(item) {
    this.pending.delete(item.id);
    this.#append({ op: 'd', id: item.id });
    this.doneSinceCompact += 1;
    if (this.doneSinceCompact > 50_000 && this.journalPath) this.#rewrite([...this.pending.values()]);
  }

  kick() {
    if (this.paused) return;
    const now = Date.now();
    let progressed = true;
    let soonest = Infinity;
    while (progressed) {
      progressed = false;
      for (let n = 0; n < this.rr.length; n++) {
        this.rrIndex = (this.rrIndex + 1) % this.rr.length;
        const systemId = this.rr[this.rrIndex];
        const l = this.lanes.get(systemId);
        if (l.active >= l.limit || l.items.length === 0) continue;
        const idx = l.items.findIndex((it) => it.nextAt <= now);
        if (idx === -1) {
          for (const it of l.items) soonest = Math.min(soonest, it.nextAt);
          continue;
        }
        const [item] = l.items.splice(idx, 1);
        this.#run(l, item);
        progressed = true;
      }
    }
    if (soonest !== Infinity && !this.timer) {
      this.timer = setTimeout(() => { this.timer = null; this.kick(); }, Math.max(50, soonest - now));
      this.timer.unref?.();
    }
  }

  async #run(l, item) {
    l.active += 1;
    item.attempts += 1;
    const started = Date.now();
    let outcome;
    try {
      outcome = await this.handler(item);
    } catch (e) {
      this.log(`[queue] handler error ${item.id}: ${e?.message || e}`);
      outcome = { retryInMs: 5000 };
    }
    const ms = Date.now() - started;
    l.active -= 1;
    this.#adapt(l, ms);
    if (outcome?.retryInMs != null) {
      l.retried += 1;
      item.nextAt = Date.now() + outcome.retryInMs;
      l.items.push(item);
    } else {
      l.done += 1;
      this.#finish(item);
    }
    this.kick();
  }

  // Additive increase / multiplicative decrease on backend call time.
  #adapt(l, ms) {
    l.lastMs.push(ms);
    if (l.lastMs.length < 20) return;
    const sorted = [...l.lastMs].sort((a, b) => a - b);
    const p50 = sorted[Math.floor(sorted.length / 2)];
    l.lastMs = [];
    l.p50 = p50;
    if (p50 > this.slowMs) l.limit = Math.max(this.minConcurrency, Math.floor(l.limit * 0.75));
    else if (p50 < this.fastMs) l.limit = Math.min(this.maxConcurrency, l.limit + 4);
  }

  stats() {
    const now = Date.now();
    const out = {};
    for (const [systemId, l] of this.lanes) {
      const oldest = l.items.reduce((m, it) => Math.min(m, it.receivedAt || now), now);
      out[systemId] = {
        queued: l.items.length,
        sending: l.active,
        concurrency: l.limit,
        backend_p50_ms: l.p50 ?? null,
        oldest_age_s: Math.round((now - oldest) / 1000),
        sent_or_closed: l.done,
        retries: l.retried,
      };
    }
    return { pending: this.pending.size, journal: this.journalPath, lanes: out };
  }

  activeCount() {
    let n = 0;
    for (const l of this.lanes.values()) n += l.active;
    return n;
  }

  close() {
    this.paused = true;
    clearTimeout(this.timer);
    if (this.fd != null) { try { fs.fsyncSync(this.fd); fs.closeSync(this.fd); } catch {} this.fd = null; }
  }
}
