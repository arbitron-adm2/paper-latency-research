'use strict';
// Public market data only. No account, signer, order transport or recording collector.
const { paperQuote } = require('./paper-quote.cjs');
const INSTRUMENTS = Object.freeze({
  BTC: Object.freeze({ symbol: 'BTC', instrumentId: 0, priceScale: 10, priceDigits: 1, pair: 'BTCUSDT' }),
  ETH: Object.freeze({ symbol: 'ETH', instrumentId: 1, priceScale: 100, priceDigits: 2, pair: 'ETHUSDT' }),
});
const SOURCES = ['hl', 'bf', 'bs'];
const finite = value => typeof value === 'number' && Number.isFinite(value);
const blank = () => ({ bid: null, ask: null, t: null, sourceTime: null, quoteKind: null, bids: [], asks: [], bookT: null, bookSourceTime: null });
function symbolOfPair(pair) { return Object.keys(INSTRUMENTS).find(s => INSTRUMENTS[s].pair === String(pair).toUpperCase()) || null; }
function levels(rows) {
  if (!Array.isArray(rows)) throw new Error('Invalid levels');
  return rows.slice(0, 20).map(row => {
    const px = Number(Array.isArray(row) ? row[0] : row.px), qty = Number(Array.isArray(row) ? row[1] : row.sz);
    if (!(px > 0) || !(qty >= 0) || !Number.isFinite(px + qty)) throw new Error('Invalid level');
    return [px, qty];
  });
}
class MarketData {
  constructor({ now = Date.now } = {}) {
    this.now = now;
    this.listeners = new Set();
    this.data = Object.fromEntries(Object.keys(INSTRUMENTS).map(s => [s, { feeds: Object.fromEntries(SOURCES.map(v => [v, blank()])), paper: null }]));
  }
  onEvent(fn) { this.listeners.add(fn); return () => this.listeners.delete(fn); }
  publish(symbol, venue, frameKind, sourceTime = null) {
    if (!this.listeners.size) return;
    const f = this.data[symbol].feeds[venue], spec = INSTRUMENTS[symbol];
    const payload = { instrumentId: spec.instrumentId, priceScale: spec.priceScale, frameKind, sourceTimestampKnown: finite(sourceTime),
      bid: String(frameKind === 'book' ? f.bids[0][0] : f.bid), ask: String(frameKind === 'book' ? f.asks[0][0] : f.ask) };
    if (frameKind === 'quote') payload.quoteKind = f.quoteKind;
    if (frameKind === 'book') { payload.bids = f.bids.map(row => row.map(String)); payload.asks = f.asks.map(row => row.map(String)); }
    const event = { kind: 'market', symbol, venue, recv_ms: this.now(), source_ms: finite(sourceTime) ? sourceTime : null, payload };
    for (const fn of this.listeners) { try { fn(event); } catch {} }
  }
  quote(symbol, source, bid, ask, sourceTime = null, quoteKind = 'bookTicker') {
    const market = this.data[symbol], b = Number(bid), a = Number(ask), now = this.now();
    if (!market || !SOURCES.includes(source) || !(b > 0) || !(a >= b) || !Number.isFinite(a + b) || (finite(sourceTime) && sourceTime > now + 5000)) return false;
    const f = market.feeds[source];
    if (finite(sourceTime) && finite(f.sourceTime) && sourceTime < f.sourceTime) return false;
    Object.assign(f, { bid: b, ask: a, t: now, sourceTime: finite(sourceTime) ? sourceTime : null, quoteKind });
    return true;
  }
  book(symbol, source, bids, asks, sourceTime = null) {
    const market = this.data[symbol];
    if (!market || !SOURCES.includes(source)) return false;
    try {
      const b = levels(bids), a = levels(asks);
      if (!b.length || !a.length || b[0][0] > a[0][0]) return false;
      const f = market.feeds[source], now = this.now();
      if (finite(sourceTime) && (sourceTime > now + 5000 || (finite(f.bookSourceTime) && sourceTime < f.bookSourceTime))) return false;
      Object.assign(f, { bids: b, asks: a, bookT: now, bookSourceTime: finite(sourceTime) ? sourceTime : null });
      return true;
    } catch { return false; }
  }
  hyperliquid(message) {
    const d = message && message.data, symbol = d && d.coin;
    if (!Object.hasOwn(INSTRUMENTS, symbol)) return null;
    if (message.channel === 'bbo' && Array.isArray(d.bbo) && d.bbo.length === 2) {
      const [b, a] = d.bbo;
      if (!b || !a) return null;
      const sourceTime = Number(d.time);
      if (!(sourceTime > 0) || !this.quote(symbol, 'hl', b.px, a.px, sourceTime, 'bbo')) return null;
      try { this.data[symbol].paper = paperQuote(d, this.now(), INSTRUMENTS[symbol].priceScale, symbol); } catch { this.data[symbol].paper = null; }
    } else if (message.channel === 'l2Book' && Array.isArray(d.levels)) {
      if (!this.book(symbol, 'hl', d.levels[0], d.levels[1], Number(d.time) > 0 ? Number(d.time) : null)) return null;
    } else return null;
    this.publish(symbol, 'hl', message.channel === 'l2Book' ? 'book' : 'quote', Number(d.time) > 0 ? Number(d.time) : null);
    return symbol;
  }
  binance(source, message) {
    if (!['bf', 'bs'].includes(source) || !message) return null;
    const d = message.data || message;
    const stream = typeof message.stream === 'string' ? message.stream : '';
    const streamSymbol = stream ? symbolOfPair(stream.split('@')[0]) : null;
    const symbol = symbolOfPair(d.s) || streamSymbol;
    if (!symbol || (streamSymbol && d.s && streamSymbol !== symbol)) return null;
    if (d.e === 'bookTicker' || stream.endsWith('@bookTicker')) {
      const t = Number(d.T || d.E);
      if (!this.quote(symbol, source, d.b, d.a, finite(t) && t > 0 ? t : null)) return null;
    } else if (d.e === 'depthUpdate' || stream.includes('@depth20')) {
      const t = Number(d.T || d.E), timestamp = finite(t) && t > 0 ? t : null;
      if (!this.book(symbol, source, d.bids || d.b, d.asks || d.a, timestamp)) return null;
      const f = this.data[symbol].feeds[source];
      // Futures depth20 is a complete partial snapshot, including real best prices.
      // Use its L1 only when the BBO path trails it by >300 ms (or is absent).
      if (source === 'bf' && timestamp !== null && this.now() - timestamp <= 1500 &&
          (f.sourceTime === null || timestamp - f.sourceTime > 300)) {
        if (this.quote(symbol, source, f.bids[0][0], f.asks[0][0], timestamp, 'depth20-top')) this.publish(symbol, source, 'quote', timestamp);
      }
    } else return null;
    this.publish(symbol, source, d.e === 'depthUpdate' || stream.includes('@depth20') ? 'book' : 'quote', Number(d.T || d.E) > 0 ? Number(d.T || d.E) : null);
    return symbol;
  }
  snapshot(maxFeedAgeMs = 1500) {
    const now = this.now();
    return Object.fromEntries(Object.entries(INSTRUMENTS).map(([symbol, spec]) => {
      const market = this.data[symbol], feeds = {};
      for (const source of SOURCES) {
        const f = market.feeds[source];
        const ageMs = f.t === null ? null : Math.max(0, now - f.t, finite(f.sourceTime) ? now - f.sourceTime : 0);
        const bookAgeMs = f.bookT === null ? null : Math.max(0, now - f.bookT, finite(f.bookSourceTime) ? now - f.bookSourceTime : 0);
        const mid = finite(f.bid) && finite(f.ask) ? (f.bid + f.ask) / 2 : null;
        let imb = null, l1b = null, l1a = null;
        if (mid && bookAgeMs !== null && bookAgeMs <= (source === 'hl' ? 10000 : 3000) && f.bids.length && f.asks.length) {
          const lo = mid * (1 - .0002), hi = mid * (1 + .0002);
          const b = f.bids.filter(([p]) => p >= lo).reduce((n, [p, q]) => n + p * q, 0);
          const a = f.asks.filter(([p]) => p <= hi).reduce((n, [p, q]) => n + p * q, 0);
          imb = b + a > 0 ? (b - a) / (b + a) : null;
          l1b = f.bids[0][0] * f.bids[0][1]; l1a = f.asks[0][0] * f.asks[0][1];
        }
        feeds[source] = { bid: f.bid, ask: f.ask, mid, ageMs, bookAgeMs, imb, l1b, l1a, bookNA: false,
          sourceTimeMs: f.sourceTime, receivedAtMs: f.t, quoteKind: f.quoteKind, source: source === 'hl' ? 'Hyperliquid BBO (raw, no smoothing)' : `Binance ${source === 'bf' ? 'Futures' : 'Spot'} ${spec.pair}${f.quoteKind === 'depth20-top' ? ' (depth20 best prices)' : ''}` };
      }
      feeds.paper = market.paper ? { ...market.paper, ageMs: Math.max(0, now - market.paper.t, now - market.paper.sourceTimeMs) }
        : { mid: null, bid: null, ask: null, ageMs: null, bookAgeMs: null, bookNA: true, imb: null, source: 'Waiting for verified website BBO conversion' };
      const fresh = source => finite(feeds[source].mid) && finite(feeds[source].ageMs) && feeds[source].ageMs <= maxFeedAgeMs;
      const spread = (a, b) => fresh(a) && fresh(b) ? feeds[a].mid - feeds[b].mid : null;
      const paperSpread = spread('paper', 'bf'), hlSpread = spread('hl', 'bf');
      return [symbol, { ...spec, observedAt: new Date(now).toISOString(), feeds, maxFeedAgeMs,
        marketFresh: fresh('hl') && fresh('bf'), paperFresh: fresh('paper') && fresh('bf'),
        paperSpread, hlSpread, paperSpreadBps: paperSpread === null ? null : paperSpread / feeds.bf.mid * 10000,
        hlSpreadBps: hlSpread === null ? null : hlSpread / feeds.bf.mid * 10000,
        sourceNote: 'Reuses the strategy WebSockets; no second recording collector. Website quote is not a future execution price.' }];
    }));
  }
}
const marketData = new MarketData();
module.exports = { MarketData, marketData, INSTRUMENTS, symbolOfPair };
