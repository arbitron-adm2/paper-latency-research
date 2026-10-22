'use strict';
const test = require('node:test'), assert = require('node:assert/strict');
const fs = require('node:fs'), os = require('node:os'), path = require('node:path'), crypto = require('node:crypto');
const { DatabaseSync } = require('node:sqlite');
const { CandidateRuntime } = require('../../apps/vchera2/btc-long-runtime.cjs');
const { PROFILE_HASH } = require('../../apps/vchera2/btc-long-candidate.cjs');
const safety = require('../../apps/vchera2/safety.cjs');
function fixture(t) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'btc-long-runtime-test-'));
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const db = new DatabaseSync(':memory:'); t.after(() => db.close());
  db.exec('CREATE TABLE btcy2_lag_trades(strategy TEXT,live INTEGER,status TEXT,qty REAL,real_pnl REAL,closed_at INTEGER)');
  const rows = Array.from({ length: 1200 }, (_, i) => ({ t: i * 1000, hl: 100000, bf: 100000,
    hl_age: 100, bf_age: 100, hl_imb: .9, bf_imb: .9 }));
  const seedFile = path.join(dir, 'seed.json'); fs.writeFileSync(seedFile, JSON.stringify({ profileHash: PROFILE_HASH,
    symbol: 'BTC', scope: 'OWN_CANONICAL_ONLY', cutMs: 1199000, rows }));
  const options = { db, seedFile, seedSha256: crypto.createHash('sha256').update(fs.readFileSync(seedFile)).digest('hex'),
    marketData: { onEvent: () => () => {} }, now: () => 1200000 };
  return { db, options, runtime: new CandidateRuntime(options) };
}
test('pending intent survives runtime reconstruction with exact identity and without resubmission', t => {
  const { db, options, runtime } = fixture(t);
  const q = { intentId: '0xabc', createdAt: 1199000, validUntilMs: 1209000, margin: 100, leverage: 500, ctx: { hl: 100000, sig: 1 } };
  runtime.remember(q);
  const restarted = new CandidateRuntime(options), pending = [];
  const args = { pending, positions: new Map(), cancelledLive: new Map(), provider: { entrySafety: { unknown: [] }, wallet: { operations: [], openPositions: [{ intentId: '0xforeign' }] } } };
  restarted.recover(args); assert.equal(pending.length, 0);
  args.provider.entrySafety.unknown = [{ intentId: '0xabc' }];
  restarted.recover(args); restarted.recover(args);
  assert.equal(pending.length, 1); assert.equal(pending[0].recovered, true); assert.equal(pending[0].intentId, '0xabc');
  assert.equal(pending[0].createdAt, q.createdAt); assert.equal(pending[0].validUntilMs, q.validUntilMs);
  restarted.mark('0xabc', 'filled', '42');
  assert.equal(db.prepare('SELECT state FROM btc_long_candidate_intents').get().state, 'filled');
});
test('unreconciled own close blocks entries and cooldown is based on actual closed time', t => {
  const { db, runtime } = fixture(t);
  db.prepare('INSERT INTO btcy2_lag_trades VALUES(?,1,?,1,NULL,?)').run('btc_long_quiet_v1', 'closed', 1199000);
  assert.ok(runtime.blockers().includes('CANDIDATE_CLOSED_PNL_NOT_RECONCILED'));
  assert.equal(runtime.nextEntryAt(), 1231000);
  db.prepare('UPDATE btcy2_lag_trades SET real_pnl=1').run();
  assert.ok(!runtime.blockers().includes('CANDIDATE_CLOSED_PNL_NOT_RECONCILED'));
});
test('warmup hash mismatch prevents arming instead of accepting unverified history', t => {
  const { options } = fixture(t);
  const r = new CandidateRuntime({ ...options, seedSha256: '0'.repeat(64) });
  assert.ok(r.blockers().includes('CANDIDATE_SEED_HASH'));
});
test('candidate approval is bound to exact rules, selected amount, leverage, expiry and aggregate cap', () => {
  const c = structuredClone(safety.config), rules = require('../../apps/vchera2/btc-long-candidate.cjs');
  delete c.mainStrategy;delete c.y2Strategy;
  c.candidate = { enabled: true, id: rules.ID, profileHash: PROFILE_HASH, seedSha256: 'a'.repeat(64) };
  c.strategy = { ...c.strategy, margin: 100, leverage: 500, entryMaxWaitMs: 8000, cancelIfSignalGone: false, cancelMissedUsd: 0 };
  c.independentGuards = { ...c.independentGuards, maximumAggregateNotionalUsd: 50000, dayHighWaterDrawdownUsd: 200, entrySignedValidityMs: 8000 };
  c.liveTestApproval = { ...c.liveTestApproval, candidate: { approved: true, profileHash: PROFILE_HASH, margin: 100, leverage: 500 } };
  assert.equal(safety.assertLaunchConfig(c), true);
  for (const mutate of [x => x.strategy.margin = 201, x => x.strategy.leverage = 701,
    x => x.candidate.profileHash = 'b'.repeat(64), x => x.independentGuards.maximumAggregateNotionalUsd = 100000,
    x => x.independentGuards.entrySignedValidityMs = 20000, x => x.strategy.cancelIfSignalGone = true]) {
    const invalid = structuredClone(c); mutate(invalid); assert.throws(() => safety.assertLaunchConfig(invalid));
  }
});
