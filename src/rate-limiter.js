// Simple token-bucket pacer so a customer's SMPP burst never exceeds the
// carrier drain rate (default 17 messages/sec per account).
export class RateLimiter {
  constructor(tps) {
    this.capacity = Math.max(1, tps);
    this.tokens = this.capacity;
    this.lastRefill = Date.now();
  }

  #refill() {
    const now = Date.now();
    const elapsed = (now - this.lastRefill) / 1000;
    if (elapsed <= 0) return;
    this.tokens = Math.min(this.capacity, this.tokens + elapsed * this.capacity);
    this.lastRefill = now;
  }

  async take() {
    for (;;) {
      this.#refill();
      if (this.tokens >= 1) {
        this.tokens -= 1;
        return;
      }
      const waitMs = Math.ceil(((1 - this.tokens) / this.capacity) * 1000);
      await new Promise((r) => setTimeout(r, Math.max(5, waitMs)));
    }
  }
}
