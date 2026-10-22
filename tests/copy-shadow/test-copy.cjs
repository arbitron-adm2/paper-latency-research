'use strict';
const test = require('node:test'), assert = require('node:assert'), fs = require('node:fs'), os = require('node:os'), path = require('node:path');
const ROOT = path.resolve(__dirname, '../..');
const { settle, UNIT } = require(path.join(ROOT, 'apps/papertrade-shadow/money.cjs'));
const { Runner, openDb, WALLET, RULES } = require(path.join(ROOT, 'apps/copy-shadow/copy-6ec3.cjs'));
const PARAMS = JSON.parse(fs.readFileSync(path.join(ROOT, 'apps/papertrade-shadow/native-parameters.json'), 'utf8'));
const MONEY = { payoutMode: 'reconstructed-deadband-haircut', instrumentRow: PARAMS.instruments.find(r => r[1] === 'ETH'), winFeeRaw: PARAMS.winFeeRaw, fundingBps: 0 };

test('money.cjs reproduces every public 0x6ec3 ETH close exactly', () => {
  const items = JSON.parse(fs.readFileSync(path.join(ROOT, 'reports/wallet-6ec3-20261021/raw/trades.json'), 'utf8')).items.filter(r => r.symbol === 'ETH');
  assert.ok(items.length >= 177);
  for (const r of items) {
    const got = settle({ margin: BigInt(r.marginRaw), leverage: r.leverage, entry: BigInt(r.entryPriceRaw), exit: BigInt(r.exitPriceRaw), long: r.isLong }, MONEY).cash;
    assert.strictEqual(got, BigInt(r.adjustedPnlRaw) - BigInt(r.userPaidFeeRaw), r.positionId);
  }
});

const ev = (kind, pid, side, ts, extra = {}) => ({ wallet: WALLET, instrument: 'ETH', kind, positionId: pid, side, leverage: 500, notionalUsd: 5e6, ts, ...extra });
const fixed = R => R.models.find(m => m.cfg.id === 'copy_6ec3_fixed_6_10');
function quotes(R, from, to, px) { for (let t = from; t <= to; t += 500) R.quote(t, px(t) - 0.05, px(t) + 0.05); }

test('fixed model: fill at HL mid +6 s, exit +10 s after observed close, Paper payout', () => {
  const R = new Runner(openDb(':memory:')), m = fixed(R), t0 = 1_000_000;
  quotes(R, t0 - 5000, t0, () => 2450);
  R.trade(t0, ev('open', 1, 'long', t0 - 1000));
  quotes(R, t0 + 500, t0 + 60000, t => t < t0 + 6000 ? 2450 : 2451.0);
  R.trade(t0 + 60000, ev('close', 1, 'long', t0 + 59000));
  quotes(R, t0 + 60500, t0 + 80000, t => t < t0 + 70000 ? 2451.0 : 2452.0);
  const leg = R.db.prepare(`SELECT * FROM copy_legs WHERE model_id='copy_6ec3_fixed_6_10'`).get();
  assert.strictEqual(leg.status, 'closed'); assert.strictEqual(leg.entry_px, 2451); assert.strictEqual(leg.exit_px, 2452);
  assert.strictEqual(leg.filled_ms, t0 + 6000); assert.strictEqual(leg.closed_ms, t0 + 70000);
  const want = settle({ margin: BigInt(RULES.marginUsdc) * UNIT, leverage: 500, entry: 245100n, exit: 245200n, long: true }, MONEY).cash;
  assert.strictEqual(BigInt(leg.cash_raw), want); assert.ok(want > 0n);
  assert.strictEqual(m.cash, BigInt(RULES.capitalUsdc) * UNIT + want); assert.strictEqual(m.reserved, 0n);
});

test('close seen >=1.5 s before our fill cancels; later close yields a late fill closed at once', () => {
  const R = new Runner(openDb(':memory:')), m = fixed(R), t0 = 2_000_000;
  quotes(R, t0 - 2000, t0, () => 2400);
  R.trade(t0, ev('open', 7, 'short', t0)); R.trade(t0 + 2000, ev('close', 7, 'short', t0 + 2000));
  assert.strictEqual(m.counts.cancelled, 1); assert.strictEqual(m.reserved, 0n);
  R.trade(t0 + 3000, ev('open', 8, 'short', t0 + 3000)); quotes(R, t0 + 3000, t0 + 8000, () => 2400);
  R.trade(t0 + 8000, ev('close', 8, 'short', t0 + 8000)); quotes(R, t0 + 8000, t0 + 30000, () => 2400);
  assert.strictEqual(m.counts.lateFills, 1); assert.strictEqual(m.counts.closed, 1); assert.strictEqual(m.reserved, 0n);
});

test('liquidation on HL mid beyond bust charges full margin', () => {
  const R = new Runner(openDb(':memory:')), m = fixed(R), t0 = 3_000_000;
  quotes(R, t0 - 1000, t0, () => 2400); R.trade(t0, ev('open', 9, 'long', t0)); quotes(R, t0 + 500, t0 + 7000, () => 2400);
  quotes(R, t0 + 7500, t0 + 9000, () => 2394); // -25 bp > 1/500
  assert.strictEqual(m.counts.liquidations, 1);
  assert.strictEqual(m.cash, BigInt(RULES.capitalUsdc - RULES.marginUsdc) * UNIT);
});

test('no fresh HL price for 60 s marks unknown entry and releases margin', () => {
  const R = new Runner(openDb(':memory:')), m = fixed(R), t0 = 4_000_000;
  R.trade(t0, ev('open', 10, 'long', t0)); R.tick(t0 + 70000);
  assert.strictEqual(m.counts.unknownEntry, 1); assert.strictEqual(m.reserved, 0n); assert.strictEqual(m.cash, BigInt(RULES.capitalUsdc) * UNIT);
});

test('other wallets and BTC are ignored; restart censors open exposure', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'copy6ec3-')), f = path.join(dir, 'c.sqlite');
  let R = new Runner(openDb(f)); const t0 = 5_000_000;
  quotes(R, t0 - 1000, t0 + 8000, () => 2400);
  R.trade(t0, { ...ev('open', 11, 'long', t0), wallet: '0x0000000000000000000000000000000000000001' });
  R.trade(t0, { ...ev('open', 12, 'long', t0), instrument: 'BTC' });
  R.trade(t0, ev('open', 13, 'long', t0)); quotes(R, t0 + 8000, t0 + 9000, () => 2400); R.save();
  assert.strictEqual(fixed(R).counts.walletOpens, 1); assert.strictEqual(fixed(R).counts.fills, 1);
  R.db.close(); R = new Runner(openDb(f));
  assert.strictEqual(fixed(R).counts.censoredRestart, 1); assert.strictEqual(fixed(R).reserved, 0n);
  assert.strictEqual(R.db.prepare(`SELECT status FROM copy_legs WHERE model_id='copy_6ec3_fixed_6_10'`).get().status, 'censored_restart');
});
