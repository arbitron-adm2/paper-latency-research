'use strict';
const crypto = require('node:crypto');
const finite = value => typeof value === 'number' && Number.isFinite(value);
function sourceTime(payload) {
  const p = payload && (payload.snapshot || payload);
  if (!p || typeof p !== 'object') return null;
  for (const key of ['timestampMs', 'ts', 'time', 'timestamp']) { const value = Number(p[key]); if (finite(value) && value >= 1e12) return value; }
  return null;
}
class ResearchFeed {
  constructor({ now = Date.now, maxBytes = 64 * 1024 * 1024, maxEvents = 50000, connection = () => ({}) } = {}) {
    this.now = now; this.maxBytes = maxBytes; this.maxEvents = maxEvents; this.connection = connection;
    this.bootId = crypto.randomUUID(); this.sequence = 0; this.events = []; this.head = 0; this.bytes = 0; this.dropped = 0; this.lastDroppedSequence = 0;
    this.counts = {}; this.lastByKind = {}; this.unsubscribers = [];
  }
  publish(input) {
    if (!input || !['market', 'pressure', 'trade'].includes(input.kind) || !finite(input.recv_ms)) return false;
    const payloadJSON = JSON.stringify(input.payload);
    if (typeof payloadJSON !== 'string') return false;
    const sequence = ++this.sequence;
    const row = { schemaVersion: 1, sequence, kind: input.kind, symbol: ['BTC', 'ETH'].includes(input.symbol) ? input.symbol : null,
      venue: ['hl', 'bf', 'bs', 'paper'].includes(input.venue) ? input.venue : 'paper', recv_ms: input.recv_ms,
      source_ms: finite(input.source_ms) ? input.source_ms : null, payload: JSON.parse(payloadJSON),
      payload_sha256: crypto.createHash('sha256').update(payloadJSON).digest('hex'),
      provenance: 'Direct public WS reused by dashboard; normalised market payload / original parsed public evidence, NOT a raw-wire hash' };
    const size = Buffer.byteLength(JSON.stringify(row));
    if (size > this.maxBytes) { this.dropped++; this.lastDroppedSequence = sequence; return false; }
    if (row.kind === 'market') { const type = row.payload.frameKind === 'book' ? 'depth' : 'quotes'; this.counts[type] = (this.counts[type] || 0) + 1; }
    this.events.push({ row, size }); this.bytes += size;
    this.counts[row.kind] = (this.counts[row.kind] || 0) + 1; this.lastByKind[row.kind] = row.recv_ms;
    while (this.events.length - this.head > this.maxEvents || this.bytes > this.maxBytes) {
      this.bytes -= this.events[this.head].size; this.events[this.head++] = null;
    }
    // Amortised eviction: avoid shifting thousands of entries on every market tick.
    if (this.head >= 1024 && this.head * 2 >= this.events.length) { this.events = this.events.slice(this.head); this.head = 0; }
    return true;
  }
  attach(marketData, account) {
    this.unsubscribers.push(marketData.onEvent(event => this.publish(event)));
    if (account && account.onPublicEvidence) this.unsubscribers.push(account.onPublicEvidence(event => this.observePublic(event)));
    return this;
  }
  observePublic(event) {
    if (event.channel === 'pressure:current') return this.publish({ kind: 'pressure', symbol: null, venue: 'paper', recv_ms: event.recv_ms, source_ms: sourceTime(event.payload), payload: event.payload });
    if (event.channel !== 'public:trades') return false;
    const trades = Array.isArray(event.payload) ? event.payload : [event.payload];
    for (const trade of trades) {
      if (!trade || typeof trade !== 'object') continue;
      const id = trade.instrumentId === 0 || trade.instrumentId === '0' ? 0 : trade.instrumentId === 1 || trade.instrumentId === '1' ? 1 : null;
      const symbol = id === 0 ? 'BTC' : id === 1 ? 'ETH' : ['BTC', 'ETH'].includes(trade.symbol) ? trade.symbol : null;
      this.publish({ kind: 'trade', symbol, venue: 'paper', recv_ms: event.recv_ms, source_ms: sourceTime(trade), payload: trade });
    }
    return true;
  }
  status() {
    const now = this.now();
    return { bootId: this.bootId, mode: 'READ_ONLY_RESEARCH_NO_ORDERS', lastSequence: this.sequence,
      retainedEvents: this.events.length - this.head, retainedBytes: this.bytes, counts: { ...this.counts }, rejectedOversized: this.dropped,
      agesMs: Object.fromEntries(Object.entries(this.lastByKind).map(([key, at]) => [key, Math.max(0, now - at)])),
      newExternalConnections: 0, tradingMutations: 0, continuity: 'Bounded RAM ring; clients MUST check bootId, gap and each sequence.' };
  }
  read(after = 0, limit = 128) {
    if (!Number.isSafeInteger(after) || after < 0 || !Number.isSafeInteger(limit) || limit < 1 || limit > 256) throw new Error('Invalid research cursor/limit');
    const firstSequence = this.events.length > this.head ? this.events[this.head].row.sequence : this.sequence + 1;
    let lo = this.head, hi = this.events.length;
    while (lo < hi) { const mid = (lo + hi) >>> 1; if (this.events[mid].row.sequence <= after) lo = mid + 1; else hi = mid; }
    const events = this.events.slice(lo, lo + limit).map(item => item.row);
    return { schemaVersion: 1, bootId: this.bootId, observedAtMs: this.now(), firstSequence, lastSequence: this.sequence,
      nextAfter: events.length ? events.at(-1).sequence : after,
      gap: after < firstSequence - 1 || after > this.sequence || after < this.lastDroppedSequence,
      events: JSON.parse(JSON.stringify(events)), connection: this.connection(), status: this.status(), mode: 'READ_ONLY_RESEARCH_NO_ORDERS' };
  }
  close() { for (const unsubscribe of this.unsubscribers) if (typeof unsubscribe === 'function') unsubscribe(); this.unsubscribers = []; }
}
module.exports = { ResearchFeed, sourceTime };
