'use strict';
const test = require('node:test'), assert = require('node:assert/strict');
const { MarketData, INSTRUMENTS } = require('../../apps/vchera2/market-data.cjs');
const { paperQuote } = require('../../apps/vchera2/paper-quote.cjs');
const { InstrumentSelection } = require('../../apps/vchera2/instrument-selection.cjs');
const bbo = (coin, bid, ask, time = 10000) => ({ channel: 'bbo', data: { coin, time, bbo: [{ px: String(bid), sz: '2' }, { px: String(ask), sz: '1' }] } });
function router(read, save = () => {}) { return new InstrumentSelection({ file: '/virtual/instruments.json', save, read: read || (() => { const e = new Error(); e.code = 'ENOENT'; throw e; }) }); }
const flat = () => ({ LIVE: false, entriesEnabled: false, providerFresh: true, openManaged: [], provider: { wallet: { openPositions: [], operations: [] } } });
test('verified instrument routing: BTC=0/10, ETH=1/100; no inherited ETH trading profile', () => {
  assert.equal(INSTRUMENTS.BTC.instrumentId, 0); assert.equal(INSTRUMENTS.ETH.instrumentId, 1);
  assert.equal(INSTRUMENTS.ETH.priceScale, 100);
  const eth = router().context('ETH'); assert.equal(eth.strategyReady, false); assert.equal(eth.exchangeMutationsEnabled, false);
  assert.match(eth.parametersSource, /NOT copied/); assert.throws(() => router().context('SOL'));
});
test('ETH website quote has exact scale-100 midpoint; BTC scale does not leak', () => {
  const q = paperQuote(bbo('ETH', '3312.11', '3312.12').data, 10000, 100, 'ETH');
  assert.equal(q.mid, 3312.11); assert.equal(q.symbol, 'ETH'); assert.match(q.source, /ETH scale 100/);
  assert.throws(() => paperQuote(bbo('ETH', 3300, 3301).data));
  assert.throws(() => paperQuote(bbo('ETH', 3300, 3301).data, 10000, 10, 'ETH'));
});
test('both HL instruments remain isolated including books and website quotes', () => {
  const m = new MarketData({ now: () => 10000 });
  assert.equal(m.hyperliquid(bbo('BTC', 81500.1, 81500.2)), 'BTC');
  assert.equal(m.hyperliquid(bbo('ETH', 3300.01, 3300.02)), 'ETH');
  m.hyperliquid({ channel: 'l2Book', data: { coin: 'ETH', levels: [[{ px: '3300.01', sz: '3' }], [{ px: '3300.02', sz: '1' }]] } });
  const s = m.snapshot(); assert.equal(s.BTC.feeds.hl.mid, 81500.15); assert.equal(s.ETH.feeds.paper.mid, 3300.01);
  assert.equal(s.BTC.feeds.hl.imb, null); assert.ok(s.ETH.feeds.hl.imb > 0);
  assert.equal(s.ETH.feeds.paper.bookNA, true); assert.equal(s.ETH.feeds.paper.imb, null);
});
test('Binance combined streams and direct payloads route by explicit symbol; mismatch and unknown rejected', () => {
  const m = new MarketData({ now: () => 10000 });
  assert.equal(m.binance('bf', { stream: 'ethusdt@bookTicker', data: { e: 'bookTicker', s: 'ETHUSDT', b: '3300', a: '3301' } }), 'ETH');
  assert.equal(m.binance('bs', { stream: 'btcusdt@bookTicker', data: { b: '81500', a: '81501' } }), 'BTC');
  assert.equal(m.binance('bf', { s: 'BTCUSDT', e: 'bookTicker', b: '81499', a: '81500' }), 'BTC');
  assert.equal(m.binance('bf', { stream: 'ethusdt@bookTicker', data: { s: 'BTCUSDT', b: '1', a: '2' } }), null);
  assert.equal(m.binance('bs', { b: '1', a: '2' }), null);
  const s = m.snapshot(); assert.equal(s.ETH.feeds.bf.mid, 3300.5); assert.equal(s.BTC.feeds.bf.mid, 81499.5);
  assert.equal(s.ETH.feeds.bs.mid, null);
});
test('stale or missing feeds produce null spread, not a fabricated zero; source age is honoured', () => {
  let now = 10000; const m = new MarketData({ now: () => now });
  assert.equal(m.snapshot().ETH.paperSpread, null);
  m.hyperliquid(bbo('ETH', 3300, 3300.02)); m.quote('ETH', 'bf', 3299.9, 3300.1, 10000);
  assert.ok(m.snapshot().ETH.marketFresh); assert.ok(Math.abs(m.snapshot().ETH.paperSpread - .01) < 1e-8);
  now = 12000; const stale = m.snapshot(); assert.equal(stale.ETH.marketFresh, false); assert.equal(stale.ETH.paperSpread, null);
  assert.equal(stale.BTC.feeds.hl.mid, null);
});
test('old, crossed, unknown and future quotes cannot poison last good symbol state', () => {
  const m = new MarketData({ now: () => 10000 }); m.hyperliquid(bbo('BTC', 81500, 81501));
  assert.equal(m.hyperliquid(bbo('BTC', 1, 2, 9999)), null);
  assert.equal(m.hyperliquid(bbo('BTC', 81502, 81501)), null);
  assert.equal(m.hyperliquid(bbo('BTC', 81500, 81501, 20000)), null);
  assert.equal(m.hyperliquid(bbo('SOL', 1, 2)), null);
  assert.equal(m.snapshot().BTC.feeds.hl.mid, 81500.5);
});
test('BTC / ETH / BOTH selection persists and reloads without enabling trading or resetting protections', () => {
  let saved = null; const r = router(null, (_, value) => { saved = value; });
  for (const mode of ['BTC', 'ETH', 'BOTH']) {
    const result = r.change({ mode }, flat()); assert.equal(result.ok, true); assert.equal(saved.mode, mode); assert.equal(saved.enablesTrading, false);
    assert.equal(result.selection.liveReady, false); assert.ok(r.startBlockers().length);
  }
  const loaded = router(() => JSON.stringify(saved)); assert.deepEqual(loaded.snapshot().selectedSymbols, ['BTC', 'ETH']);
  assert.equal(loaded.context('BTC').selected, true); assert.equal(loaded.context('ETH').selected, true);
});
test('selection fails closed on LIVE, unknown managed outcomes, any wallet position/pending or stale wallet', () => {
  const r = router();
  const cases = [ { LIVE: true }, { entriesEnabled: true }, { providerFresh: false }, { openManaged: [{}] },
    { provider: { wallet: { openPositions: [{ instrumentId: 1 }], operations: [] } } },
    { provider: { wallet: { openPositions: [], operations: [['unknown']] } } }, { provider: null } ];
  for (const patch of cases) assert.equal(r.change({ mode: 'BOTH' }, { ...flat(), ...patch }).ok, false);
  assert.equal(r.snapshot().mode, 'BTC');
});
test('invalid/unsafe selection payload and corrupt persistent state never arm any instrument', () => {
  const r = router(() => '{invalid'); assert.ok(r.snapshot().error); assert.ok(r.startBlockers().length);
  for (const body of [{ mode: 'SOL' }, { mode: '__proto__' }, { mode: 'BTC', live: true }, [], null]) assert.equal(r.change(body, flat()).ok, false);
  const failing = router(null, () => { throw new Error('disk full'); }); assert.throws(() => failing.change({ mode: 'ETH' }, flat())); assert.equal(failing.snapshot().mode, 'BTC');
});
