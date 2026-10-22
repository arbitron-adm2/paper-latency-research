'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const safety = require('../../apps/vchera2/safety.cjs');
const funding = require('../../apps/vchera2/scanner/funding-guard.js');
const { createRegime } = require('../../apps/vchera2/scanner/regime.js');

test('launch config: approved exposure, own wallet, guards armed, absolute cutoff', () => {
  assert.equal(safety.assertLaunchConfig(safety.config), true);
  const cfg = safety.lockedStrategyConfig({ haircut: [] }, 'test-day-none');
  assert.equal(cfg.margin * cfg.leverage, 1260000);
  assert.equal(cfg.maxPositions, 1); assert.equal(cfg.splitParts, 1);
  assert.equal(cfg.noLongs, false); assert.equal(cfg.useMainKey, false);
  assert.equal(cfg.maxDayLossUsd, 0);
  assert.equal(cfg.lossHardUsd, 1000); assert.equal(cfg.lossWindow, 1); assert.equal(cfg.streakPauseMin, 10); assert.equal(cfg.maxLossStreak, 0);
  assert.equal(cfg.stopUsd, 4); assert.equal(cfg.cushionExitUsd, 4); assert.equal(cfg.binExitUsd, 8); assert.equal(cfg.trailUsd, 6.5);
  assert.equal(cfg.entryMaxWaitMs, 15000); assert.equal(cfg.cancelMissedUsd, 12);
  assert.equal(cfg.ptWallet, safety.config.wallet);
  assert.equal(safety.config.independentGuards.tradeUntilAbsolute, '2026-10-20T15:30:00+03:00');
  assert.equal(Date.parse(safety.config.independentGuards.tradeUntilAbsolute), Date.parse('2026-10-20T12:30:00Z'));
});
test('runtime patches may only switch off', () => {
  assert.throws(() => safety.rejectUnsafePatch({ live: true }));
  assert.throws(() => safety.rejectUnsafePatch({ margin: 5000 }));
  assert.doesNotThrow(() => safety.rejectUnsafePatch({ live: false }));
  assert.doesNotThrow(() => safety.rejectUnsafePatch({ live: false, enabled: false }));
});
test('broken config refused', () => {
  const bad = (patch) => assert.throws(() => safety.assertLaunchConfig({ ...safety.config, strategy: { ...safety.config.strategy, ...patch } }));
  bad({ margin: 3000 }); bad({ leverage: 700 }); bad({ maxDayLossUsd: 1500 }); bad({ splitParts: 3 }); bad({ useMainKey: true });
  assert.throws(() => safety.assertLaunchConfig({ ...safety.config, independentGuards: { ...safety.config.independentGuards, tradeUntilAbsolute: '15:30' } }));
});
test('cutoff: equality stops, naive time refused', () => {
  const until = '2026-10-20T15:30:00+03:00', at = Date.parse(until);
  assert.equal(safety.absoluteCutoff('15:30', at).blocked, true);
  assert.equal(safety.absoluteCutoff(until, at - 1).blocked, false);
  assert.equal(safety.absoluteCutoff(until, at).blocked, true);
  assert.equal(safety.cutoffReached(at - 1), false);
  assert.equal(safety.cutoffReached(at), true);
  assert.ok(safety.tradingWindowReason(at));
});
test('round levels: 50 below / 100 above', () => {
  assert.equal(safety.roundBlocked(80951), true); assert.equal(safety.roundBlocked(80950), false);
  assert.equal(safety.roundBlocked(81099), true); assert.equal(safety.roundBlocked(81100), false);
});
test('high-water: drawdown from best of day, latches, new day resets', () => {
  let st = safety.highWaterStep(null, { dayKey: 1, netPnl: 1000, limit: 1500 });
  st = safety.highWaterStep(JSON.parse(JSON.stringify(st)), { dayKey: 1, netPnl: -499, limit: 1500 });
  assert.equal(st.drawdown, 1499); assert.equal(st.tripped, false);
  st = safety.highWaterStep(st, { dayKey: 1, netPnl: -500, limit: 1500 });
  assert.equal(st.tripped, true);
  st = safety.highWaterStep(st, { dayKey: 1, netPnl: 3000, limit: 1500 });
  assert.equal(st.tripped, true);
  st = safety.highWaterStep(st, { dayKey: 2, netPnl: 0, limit: 1500 });
  assert.equal(st.tripped, false); assert.equal(st.best, 0);
  // from flat start: -1500 trips (best is at least 0)
  assert.equal(safety.highWaterStep(null, { dayKey: 3, netPnl: -1500, limit: 1500 }).tripped, true);
  assert.throws(() => safety.highWaterStep(null, { dayKey: 1, netPnl: NaN }));
});
test('funding: no entries XX:56–XX:03, close at XX:59', () => {
  funding.setCfg(safety.config.independentGuards);
  const at = (min, sec = 0) => new Date(2026, 9, 20, 10, min, sec).getTime();
  assert.equal(funding.noEntry(at(55, 59)), null); assert.ok(funding.noEntry(at(56)));
  assert.ok(funding.noEntry(at(2, 59))); assert.equal(funding.noEntry(at(3)), null);
  assert.equal(funding.mustClose(at(58, 59)), false); assert.equal(funding.mustClose(at(59)), true);
});
test('walls: 3M against within $30 blocks that side 60 s; 5M pauses all', () => {
  const g = new safety.WallGuard(); const now = 100000;
  const feed = { bid: 80000, ask: 80002, bookT: now, bids: [[80000, 40]], asks: [[80002, 1]] };
  g.update({ hl: feed }, now);
  assert.ok(g.reason('S', now)); assert.equal(g.reason('L', now), null);
  g.update({}, now + 59999); assert.ok(g.reason('S', now + 59999)); assert.equal(g.reason('S', now + 60000), null);
  const far = new safety.WallGuard(); far.update({ hl: { ...feed, bids: [[79960, 100]] } }, now); assert.equal(far.reason('S', now), null);
  const big = new safety.WallGuard(); big.update({ hl: { ...feed, bids: [[80000, 70]] } }, now);
  assert.ok(big.reason('L', now)); assert.ok(big.reason('S', now));
});
test('regime HIGH after 120 s above 129, back below ~116', () => {
  const r = createRegime({ low: 82, high: 129, hystPct: 10, holdMs: 120000 });
  r.update(100, 0); r.update(142, 1); r.update(142, 120001); assert.equal(r.regime, 'HIGH');
  r.update(115, 120002); r.update(115, 240002); assert.equal(r.regime, 'NORMAL');
});
