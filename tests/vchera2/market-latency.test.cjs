'use strict';
const test = require('node:test'), assert = require('node:assert/strict');
const { MarketData } = require('../../apps/vchera2/market-data.cjs');
const { ResearchFeed } = require('../../apps/vchera2/research-feed.cjs');
const depth = (t, bid = '100', ask = '101') => ({ e: 'depthUpdate', s: 'ETHUSDT', T: t, b: [[bid, '3']], a: [[ask, '2']] });
test('fresh futures partial depth supplies actual BBO while ticker is backlogged; stale ticker cannot undo it', () => {
  let now = 10000; const m = new MarketData({ now: () => now }), broker = new ResearchFeed().attach(m);
  m.binance('bf', { e: 'bookTicker', s: 'ETHUSDT', T: 5000, b: '90', a: '91' });
  m.binance('bf', depth(9800));
  let f = m.snapshot().ETH.feeds.bf; assert.equal(f.mid, 100.5); assert.equal(f.ageMs, 200); assert.equal(f.quoteKind, 'depth20-top');
  const fallback = broker.read().events.find(e => e.payload.quoteKind === 'depth20-top'); assert.ok(fallback); assert.equal(fallback.source_ms, 9800);
  assert.equal(m.binance('bf', { e: 'bookTicker', s: 'ETHUSDT', T: 6000, b: '90', a: '91' }), null);
  now = 10100; m.binance('bf', { e: 'bookTicker', s: 'ETHUSDT', T: 10000, b: '102', a: '103' });
  f = m.snapshot().ETH.feeds.bf; assert.equal(f.mid, 102.5); assert.equal(f.quoteKind, 'bookTicker');
});
test('old/out-of-order/future books cannot be relabelled as fresh quotes or overwrite newer depth', () => {
  const m = new MarketData({ now: () => 10000 });
  m.binance('bf', depth(5000)); assert.equal(m.snapshot().ETH.feeds.bf.mid, null); assert.equal(m.snapshot().ETH.feeds.bf.bookAgeMs, 5000);
  m.binance('bf', depth(9900)); assert.equal(m.binance('bf', depth(9800, '1', '2')), null);
  assert.equal(m.binance('bf', depth(20000)), null); assert.equal(m.snapshot().ETH.feeds.bf.mid, 100.5);
});
test('research ring preserves cursor/gap and byte accounting across repeated eviction and compaction', () => {
  const b = new ResearchFeed({ maxEvents: 9 });
  for (let i = 0; i < 5000; i++) b.publish({ kind: 'pressure', recv_ms: i, payload: { i } });
  const r = b.read(0, 3); assert.equal(r.gap, true); assert.equal(r.firstSequence, 4992);
  assert.deepEqual(r.events.map(e => e.sequence), [4992, 4993, 4994]);
  assert.deepEqual(b.read(r.nextAfter, 3).events.map(e => e.sequence), [4995, 4996, 4997]);
  assert.equal(b.status().retainedEvents, 9); assert.ok(b.status().retainedBytes > 0);
});
test('dashboard state skips unused historical SQL scans while retaining live readiness fields', () => {
  const { makeHarness } = require('./execution-harness.cjs'); const h = makeHarness(); h.db.close();
  const s = h.api.getState({ light: true });
  assert.ok(s.config); assert.ok(s.feeds); assert.ok(s.executionSync); assert.ok(s.open);
  assert.equal(s.stats, undefined); assert.equal(s.compare, undefined); assert.equal(s.paper, undefined);
  assert.throws(() => h.api.getState());
});
