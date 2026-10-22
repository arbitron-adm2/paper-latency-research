'use strict';
// Pure causal rules. No signer, wallet, transport, timer or source archive imports.
const crypto = require('node:crypto');
const ID = 'btc_long_quiet_v1';
const PROFILE = Object.freeze({ side: 1, minimum_cushion: .75, maximum_cushion: 3,
  minimum_book: .8, maximum_range: 12, persistence: 3, fast_s: 120, slow_s: 1200,
  range_s: 300, minimum_fraction: .9, tp_bps: .25, sl_bps: 6,
  minimum_tp_hold_s: 20, maximum_hold_s: 120, cooldown_ms: 32000, entry_wait_ms: 8000 });
const PROFILE_HASH = crypto.createHash('sha256').update(JSON.stringify(PROFILE)).digest('hex');
const finite = x => typeof x === 'number' && Number.isFinite(x);
function median(a) { const s = a.filter(finite).sort((x, y) => x - y); return s.length ? s[s.length >> 1] : NaN; }
function bandImbalance(bids, asks, mid) {
  if (!finite(mid) || !bids?.length || !asks?.length) return NaN;
  const b = bids.slice(0, 20).filter(([p]) => +p >= mid * .9998).reduce((n, [p, q]) => n + +p * +q, 0);
  const a = asks.slice(0, 20).filter(([p]) => +p <= mid * 1.0002).reduce((n, [p, q]) => n + +p * +q, 0);
  return b + a ? Math.round((b - a) / (b + a) * 100) / 100 : 0;
}
class Candidate {
  constructor() { this.history = []; this.last = null; this.streak = 0; this.lastT = null; }
  step(row) {
    if (!Number.isSafeInteger(row.t) || row.t % 1000) throw new Error('CANDIDATE_SECOND_GRID');
    if (this.lastT !== null && row.t <= this.lastT) throw new Error('CANDIDATE_TIME_REGRESSION');
    if (this.lastT !== null && row.t !== this.lastT + 1000) { this.history = []; this.streak = 0; }
    const valid = finite(row.hl) && finite(row.bf) && finite(row.hl_age) && finite(row.bf_age) && row.hl_age <= 1500 && row.bf_age <= 1500;
    const h = valid ? row.hl : NaN, basis = valid ? (row.bf - row.hl) / row.hl * 10000 : NaN;
    const past = n => this.history.slice(-n);
    const fastRows = past(120), slowRows = past(1200), rangeRows = past(300);
    const m = (a, n) => a.length === n && a.filter(x => finite(x.basis)).length >= Math.ceil(n * .9) ? median(a.map(x => x.basis)) : NaN;
    const fast = basis - m(fastRows, 120), slow = basis - m(slowRows, 1200);
    const d = Math.sign(fast), cushion = Math.min(d * fast, d * slow);
    const prices = rangeRows.map(x => x.h).filter(finite);
    const ready = valid && rangeRows.length === 300 && prices.length >= 270;
    const range = prices.length && valid ? (Math.max(...prices) - Math.min(...prices)) / h * 10000 : NaN;
    const book = finite(row.hl_imb) && finite(row.bf_imb) ? (row.hl_imb + row.bf_imb) / 2 : NaN;
    const eligible = ready && d === 1 && cushion >= .75 && cushion <= 3 && book >= .8 && range < 12;
    this.streak = eligible ? this.streak + 1 : 0;
    this.last = { ...row, signal: this.streak >= 3 ? 1 : 0, ready, cushion, book, range,
      historySeconds: this.history.length, warmup: this.history.length < 1200,
      reason: this.history.length < 1200 ? 'WARMUP_20M' : !ready ? 'PRICE_NOT_READY' : !finite(book) ? 'BOOK_NOT_READY' : this.streak >= 3 ? 'SIGNAL_LONG' : 'NO_SIGNAL' };
    this.history.push({ t: row.t, h, basis });
    if (this.history.length > 1200) this.history.shift();
    this.lastT = row.t;
    return this.last;
  }
  seed(rows) { for (const row of rows) this.step(row); return this.last; }
  snapshot() { return { id: ID, profileHash: PROFILE_HASH, profile: PROFILE, ...(this.last || { warmup: true, reason: 'NO_ROWS' }) }; }
}
class SecondView {
  constructor({ onRow, lastRow = null } = {}) {
    this.onRow = onRow; this.next = lastRow ? lastRow.t + 1000 : null;
    this.quotes = {}; this.books = {}; this.extremes = {}; this.lastSource = {}; this.liveSeen = new Set(); this.accepted = 0; this.rejected = 0;
    if (lastRow) for (const venue of ['hl', 'bf']) {
      if (finite(lastRow[venue]) && finite(lastRow[venue + '_age'])) this.quotes[venue] = { mid: lastRow[venue], recv: lastRow.t - lastRow[venue + '_age'], source: lastRow.t - lastRow[venue + '_age'] };
    }
  }
  ingest(e) {
    if (e.kind !== 'market' || e.symbol !== 'BTC' || !['hl', 'bf'].includes(e.venue) || !Number.isSafeInteger(e.recv_ms)) return;
    if (this.next === null) this.next = Math.floor(e.recv_ms / 1000) * 1000 + 1000;
    this.advance(e.recv_ms);
    const v = e.venue, p = e.payload || {}, source = e.source_ms;
    const kind = p.frameKind;
    if (!['quote', 'book'].includes(kind) || !finite(source) || e.recv_ms - source < -200 || e.recv_ms - source > 1500 || e.recv_ms < this.next - 1000) { this.rejected++; return; }
    const key = v + ':' + kind;
    if (source < (this.lastSource[key] ?? -Infinity)) { this.rejected++; return; }
    if (kind === 'quote') {
      const bid = +p.bid, ask = +p.ask;
      if (!(bid > 0 && ask >= bid)) return;
      const mid = (bid + ask) / 2;
      this.quotes[v] = { mid, recv: e.recv_ms, source };
      const old = this.extremes[v]; this.extremes[v] = old ? [Math.min(old[0], mid), Math.max(old[1], mid)] : [mid, mid];
    } else {
      if (!p.bids?.length || !p.asks?.length) return;
      this.books[v] = { bids: p.bids, asks: p.asks, recv: e.recv_ms, source };
    }
    this.lastSource[key] = source; this.liveSeen.add(key); this.accepted++;
  }
  advance(now) {
    if (this.next === null) return;
    if (now - this.next > 3600000) {
      // Bound work after a long downtime. The causal rule resets on this gap;
      // missing prices still produce rows so existing exposure can be closed.
      this.next = Math.floor(now / 1000) * 1000 - 1200000;
      this.quotes = {}; this.books = {}; this.extremes = {}; this.liveSeen.clear();
    }
    while (this.next <= now) {
      const t = this.next, row = { t };
      for (const v of ['hl', 'bf']) {
        const q = this.quotes[v], b = this.books[v];
        row[v] = q?.mid ?? NaN;
        row[v + '_age'] = q ? Math.max(t - q.recv, t - q.source) : Infinity;
        row[v + '_low'] = this.extremes[v]?.[0] ?? NaN;
        row[v + '_high'] = this.extremes[v]?.[1] ?? NaN;
        row[v + '_book_age'] = b ? Math.max(t - b.recv, t - b.source) : Infinity;
        row[v + '_imb'] = b && row[v + '_book_age'] <= (v === 'hl' ? 10000 : 3000) ? bandImbalance(b.bids, b.asks, row[v]) : NaN;
      }
      this.extremes = {}; this.next += 1000; this.onRow(row);
    }
  }
  liveReady() { return ['hl:quote', 'bf:quote', 'hl:book', 'bf:book'].every(k => this.liveSeen.has(k)); }
}
function exitReason(position, row) {
  if (!row || !finite(position.entry) || !(position.entry > 0)) return null;
  const held = row.t - position.opened_at;
  if (held < 1000) return null;
  if (!finite(row.hl) || !(row.hl_age <= 6000)) return 'BTC LONG: нет свежей цены';
  const ep = Math.round(position.entry * 10), px = Math.floor(row.hl * 10 + 1e-7);
  const low = finite(row.hl_low) ? Math.floor(row.hl_low * 10 + 1e-7) : Infinity;
  if (low <= ep * (1 - 6 / 10000)) return 'BTC LONG: стоп 6 bps';
  if (held >= 20000 && px >= ep * (1 + .25 / 10000)) return 'BTC LONG: тейк 0.25 bps';
  if (held >= 120000) return 'BTC LONG: предел 120 с';
  return null;
}
module.exports = { ID, PROFILE, PROFILE_HASH, Candidate, SecondView, bandImbalance, exitReason };
