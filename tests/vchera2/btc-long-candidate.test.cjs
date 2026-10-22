'use strict';
const test = require('node:test'), assert = require('node:assert/strict');
const { Candidate, SecondView, exitReason, bandImbalance } = require('../../apps/vchera2/btc-long-candidate.cjs');
const row = (t, patch = {}) => ({ t, hl: 100000, bf: 100000, hl_age: 100, bf_age: 100,
  hl_low: 100000, hl_high: 100000, hl_imb: .9, bf_imb: .9, ...patch });
function warm() { const c = new Candidate(); for (let i = 0; i < 1200; i++) c.step(row(i * 1000)); return c; }
test('past-only two baselines and three consecutive confirmations', () => {
  const c = warm();
  assert.equal(c.step(row(1200000, { bf: 100010 })).signal, 0);
  assert.equal(c.step(row(1201000, { bf: 100010 })).signal, 0);
  const r = c.step(row(1202000, { bf: 100010 }));
  assert.equal(r.signal, 1); assert.equal(r.cushion, 1); assert.equal(r.warmup, false);
});
test('missing books and stale source break confirmation; continuity gaps require fresh warmup', () => {
  const c = warm();
  c.step(row(1200000, { bf: 100010 })); c.step(row(1201000, { bf: 100010 }));
  assert.equal(c.step(row(1202000, { bf: 100010, hl_imb: null })).signal, 0);
  assert.equal(c.step(row(1203000, { bf: 100010, bf_age: 1501 })).ready, false);
  assert.equal(c.step(row(1205000, { bf: 100010 })).warmup, true);
});
test('receive event exactly on second boundary is available only on next second', () => {
  const rows = [], view = new SecondView({ onRow: r => rows.push(r), lastRow: row(0) });
  view.ingest({ kind: 'market', symbol: 'BTC', venue: 'hl', recv_ms: 1000, source_ms: 1000,
    payload: { frameKind: 'quote', bid: '100010', ask: '100010' } });
  assert.equal(rows[0].t, 1000); assert.equal(rows[0].hl, 100000);
  view.advance(2000); assert.equal(rows[1].hl, 100010); assert.equal(rows[1].hl_age, 1000);
  assert.equal(view.liveReady(), false);
});
test('clock errors and regressions cannot replace a good quote', () => {
  const rows = [], view = new SecondView({ onRow: r => rows.push(r), lastRow: row(0) });
  const event = (recv, source, price) => ({ kind: 'market', symbol: 'BTC', venue: 'hl', recv_ms: recv, source_ms: source,
    payload: { frameKind: 'quote', bid: String(price), ask: String(price) } });
  view.ingest(event(1100, 1100, 100010)); view.ingest(event(1200, 1000, 90000));
  view.ingest(event(1500, null, 80000)); view.advance(2000);
  assert.equal(rows.at(-1).hl, 100010); assert.equal(view.rejected, 2);
});
test('book uses +/-2bps notional and percentage rounding', () => {
  assert.equal(bandImbalance([[100000, 9], [99900, 1000]], [[100000, 1], [100100, 1000]], 100000), .8);
  assert.ok(Number.isNaN(bandImbalance([], [[100000, 1]], 100000)));
});
test('profit minimum hold, immediate stop precedence and unconditional maximum holding time', () => {
  const p = { entry: 100000, opened_at: 0 };
  assert.equal(exitReason(p, row(19000, { hl: 100010 })), null);
  assert.match(exitReason(p, row(20000, { hl: 100010 })), /тейк/);
  assert.match(exitReason(p, row(1000, { hl: 100010, hl_low: 99930 })), /стоп/);
  assert.match(exitReason(p, row(120000)), /120/);
  assert.match(exitReason(p, row(1000, { hl_age: 6001 })), /свежей/);
});
