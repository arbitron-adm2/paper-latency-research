'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { spawnSync } = require('node:child_process');
const { TimingGate, validate, number } = require('../src/latency.js');
const dir = path.resolve(__dirname, '..');
function pressure(pending = 0, t = 10000) {
  return { snapshot: { ts: t, ok: true, stale: false, totalPendingGas: pending, totalQueuedGas: pending,
    totalSubmittedGas: 0, pendingCount: pending ? 1 : 0, chain: { blocks: [1, 2, 3].map(n => ({ number: n,
      timestampMs: t - 3000 + n * 1000, kind: 'ordinary', gasLimit: '3000000', papertradeGasUsed: '100000' })) } } };
}
const evaluate = (data, now = 10000, config = {}) => { const g = new TimingGate(config); g.observe(data, 10000); return g.evaluate(now); };

test('empty queue has explicit floors, not immediate execution or expected profit', () => {
  const r = evaluate(pressure()); assert.equal(r.allow, true); assert.equal(r.entry_ms, 3000); assert.equal(r.exit_ms, 10000);
  assert.equal(r.economics, 'NOT_MODELLED'); assert.equal(r.method, 'configured_floors_empty_queue_v1');
});
test('pending gas uses Paper service, with separate entry and exit budgets', () => {
  const r = evaluate(pressure(300000)); assert.equal(r.entry_ms, 7500); assert.equal(r.exit_ms, 14500);
  assert.deepEqual(r.reasons, ['ENTRY_DELAY_BUDGET', 'EDGE_LIFETIME_BUDGET']);
  const other = evaluate(pressure(300000), 10000, { entry_budget_ms: 10000, signal_ttl_ms: 10000, exit_budget_ms: 12000 });
  assert.deepEqual(other.reasons, ['EXIT_DELAY_BUDGET']);
});
test('only direct exchange data can pass', () => {
  const g = new TimingGate(); assert.deepEqual(g.evaluate()['reasons'], ['PRESSURE_MISSING']);
  g.observe(pressure(), 10000); const r = g.evaluate(10000, false);
  assert.equal(r.allow, false); assert.equal(r.entry_ms, null); assert.deepEqual(r.reasons, ['DIRECT_PRESSURE_UNAVAILABLE']);
});
test('stale, unhealthy and future clocks clear estimates', () => {
  assert.ok(evaluate(pressure(), 14000).reasons.includes('PRESSURE_STALE'));
  assert.ok(evaluate(pressure(), 8000).reasons.includes('PRESSURE_FROM_FUTURE'));
  const p = pressure(); p.snapshot.stale = true;
  assert.equal(evaluate(p).entry_ms, null); assert.equal(evaluate(p).allow, false);
});
test('numbers never coerce bool, blank or hex into a zero/fast queue', () => {
  for (const x of [null, undefined, true, false, '', '  ', '0x10', Infinity, NaN, -1, {}, []]) assert.equal(number(x), null);
  assert.equal(number('100000'), 100000);
  const p = pressure(); p.snapshot.totalPendingGas = false; assert.equal(evaluate(p).allow, false);
});
test('malformed optional fields and unknown block kinds fail closed', () => {
  const p = pressure(1000); p.snapshot.totalSubmittedGas = 'oops'; assert.ok(evaluate(p).reasons.includes('INVALID_GAS_OR_COUNT'));
  const q = pressure(1000); q.snapshot.chain.blocks[0].kind = 'unknown'; assert.ok(evaluate(q).reasons.includes('UNKNOWN_BLOCK_KIND'));
});
test('stalled drain, insufficient blocks and gaps are not instant execution', () => {
  const p = pressure(1000); p.snapshot.chain.blocks.forEach(b => { b.papertradeGasUsed = 0; });
  assert.deepEqual(evaluate(p).reasons, ['DRAIN_STALLED']);
  p.snapshot.chain.blocks = []; assert.deepEqual(evaluate(p).reasons, ['INSUFFICIENT_DRAIN_HISTORY']);
  const q = pressure(1000); q.snapshot.chain.blocks[2].number = 9; assert.deepEqual(evaluate(q).reasons, ['BLOCK_COVERAGE_GAP']);
});
test('repeated blocks deduplicate; regressed snapshots do not restore an allow state', () => {
  const g = new TimingGate(); g.observe(pressure(1000), 10000); g.observe(pressure(1000), 10001);
  assert.equal(g.evaluate(10001).sample_blocks, 3);
  g.observe(pressure(1000, 9999), 10002); assert.ok(g.evaluate(10002).reasons.includes('PRESSURE_SOURCE_REGRESSION'));
});
test('config is finite, positive, typed and typo-safe', () => {
  for (const c of [{ enabled: 1 }, { entry_budget_ms: true }, { safety_factor: NaN }, { window_blocks: 2.5 }, { extra: 1 }, { min_blocks: 1 }, { signal_ttl_ms: 0 }]) assert.throws(() => validate(c));
});
test('browser forecast agrees with Python reference on fixed snapshots', () => {
  const p = pressure(300000), stale = pressure(); stale.snapshot.stale = true;
  const malformed = pressure(1); malformed.snapshot.totalPendingGas = true;
  const knownBig=pressure(1000);knownBig.snapshot.chain.blocks=[1,2,3,4,5].map(number=>({number,timestampMs:5000+number*1000,kind:number===3?'big':'ordinary',papertradeGasUsed:number===3?'999999999':'100000'}));
  const missingBlock=structuredClone(knownBig);missingBlock.snapshot.chain.blocks=missingBlock.snapshot.chain.blocks.filter(x=>x.number!==3);
  const cases = [{data:knownBig,now:10000},{data:missingBlock,now:10000},{ data: pressure(), now: 10000 }, { data: p, now: 10001 }, { data: p, now: 14000 },
    { data: stale, now: 10000 }, { data: malformed, now: 10000 },
    { data: p, now: 10000, config: { entry_budget_ms: 10000, signal_ttl_ms: 10000, exit_budget_ms: 12000 } }];
  const script = `import json,sys\nsys.path.insert(0,sys.argv[1])\nfrom marketdb.latency_gate import TimingGate\nout=[]\nfor case in json.load(sys.stdin):\n g=TimingGate({'enabled':True,**case.get('config',{})})\n s=case['data']['snapshot']\n g.observe({'pressure':s,'time_ms':10000,'recv_ms':10000,'source_ms':s['ts']})\n out.append(g.evaluate(case['now']))\nprint(json.dumps(out))`;
  const referenceRoot = process.env.GUARD_REFERENCE_ROOT || path.resolve(dir, '../../.worktree/unified-market-db');
  const result = spawnSync('python', ['-c', script, referenceRoot], { input: JSON.stringify(cases), encoding: 'utf8', env: { ...process.env, PYTHONDONTWRITEBYTECODE: '1' } });
  assert.equal(result.status, 0, result.stderr);
  const expected = JSON.parse(result.stdout);
  const keys = ['status', 'allow', 'reasons', 'entry_ms', 'exit_ms', 'method', 'economics', 'sample_blocks', 'drain_gas_per_second'];
  cases.forEach((c, i) => { const actual = evaluate(c.data, c.now, c.config); for (const k of keys) assert.deepEqual(actual[k], expected[i][k], `case ${i}: ${k}`); });
});
test('built extension keeps least permissions and only public read-only paths', () => {
  const m = JSON.parse(fs.readFileSync(path.join(dir, 'manifest.json')));
  assert.equal(m.version, '1.4.0'); assert.deepEqual(m.permissions, ['storage']);
  assert.equal(m.content_scripts.length, 2); for (const c of m.content_scripts) assert.deepEqual(c.matches, ['https://exchange.papertrade.xyz/*']);
  assert.equal(m.content_scripts[0].js.at(-1), 'src/latency-panel.js');
  assert.deepEqual(m.content_scripts[1].js, ['src/chart-hook.js']); assert.equal(m.content_scripts[1].world, 'MAIN'); assert.equal(m.content_scripts[1].run_at, 'document_start');
  assert.deepEqual(m.host_permissions, ['https://exchange.papertrade.xyz/*', 'https://paperwatch.app/*']);
  const bg = fs.readFileSync(path.join(dir, 'src/background.js'), 'utf8');
  assert.equal((bg.match(/credentials: 'omit'/g) || []).length, 3);
  assert.equal(/eth_send|personal_sign|eth_sign|\/intents|method:\s*['"]POST/.test(bg), false);
  const core = require('../src/core.js'); assert.equal(core.waitText({ waitSec: 0 }), 'нет очереди'); assert.equal(core.num(true), null);
});
test('all JavaScript entrypoints parse and UI explicitly states shadow limitations', () => {
  for (const name of ['core.js', 'pump.js', 'panel.js', 'latency.js', 'latency-panel.js', 'background.js', 'binance.js', 'chart-hook.js', 'chart-overlay.js', 'spread.js', 'trader.js']) {
    const checked = spawnSync(process.execPath, ['--check', path.join(dir, 'src', name)], { encoding: 'utf8' });
    assert.equal(checked.status, 0, name + checked.stderr);
  }
  const panel = fs.readFileSync(path.join(dir, 'src/latency-panel.js'), 'utf8');
  assert.ok(panel.includes('кнопки заявок не блокируются'));
  assert.ok(panel.includes('Алерты пока не подключены'));
  assert.ok(panel.includes('нет телеметрии'));
});
