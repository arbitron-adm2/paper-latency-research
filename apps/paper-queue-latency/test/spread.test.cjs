'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const S = require('../src/spread.js');

// Модель с фоном: 60 с спреда hl−bf ≈ −2$, затем скачок. Время подбираем вне окна фандинга (XX:20).
const T0 = new Date(2026, 9, 21, 12, 20, 0).getTime();
function build({ bfNow, hlNow, bfAgo = 83500, slowFill = true } = {}) {
  const m = S.createModel();
  const hl0 = 83502;                                 // спред −2$ (hl − bf)
  const start = slowFill ? T0 - 25 * 60000 : T0 - 130000;
  for (let t = start; t < T0 - 12000; t += 500) { m.tick('f', 'BTC', t, 83500); m.sample('BTC', t, hl0); }
  for (let t = T0 - 12000; t < T0 - 9000; t += 500) { m.tick('f', 'BTC', t, bfAgo); m.sample('BTC', t, hl0); }
  for (let t = T0 - 9000; t <= T0; t += 500) { m.tick('f', 'BTC', t, t < T0 ? bfAgo + (bfNow - bfAgo) * (t - (T0 - 9000)) / 9000 : bfNow); m.sample('BTC', t, t < T0 ? hl0 : hlNow); }
  return m;
}
const ok = { timingAllow: true, paused: false, scale: 1, scaleOk: true };

test('funding и круглый уровень', () => {
  assert.ok(S.fundingBlock(new Date(2026, 9, 21, 12, 57, 0).getTime()));
  assert.ok(S.fundingBlock(new Date(2026, 9, 21, 13, 1, 0).getTime()));
  assert.equal(S.fundingBlock(new Date(2026, 9, 21, 12, 20, 0).getTime()), null);
  assert.ok(S.roundBlock(83030) && S.roundBlock(82960) && S.roundBlock(83099));
  assert.equal(S.roundBlock(83500), null);
  assert.equal(S.roundBlock(83120), null);
});

test('HL отстал вниз от Binance (Binance вырос, HL нет) → ЛОНГ при чистых правилах', () => {
  // фон −2$; Binance вверх на 5$ за 9 с (83000→83005); HL стоит 83002 → spread = −3, dev = −1 → мало. Берём Binance +9: spread −7, dev −5.
  const m = build({ bfNow: 83509, hlNow: 83502 });
  const r = m.evaluate('BTC', T0, ok);
  assert.equal(r.ready, true);
  assert.ok(r.devUsd < -8 && r.devUsd > -10, String(r.devUsd));
  assert.equal(r.side, 'L');
  assert.equal(r.rec, 'long', JSON.stringify(r.rules.filter((x) => !x.ok)));
  assert.ok(r.devBp < 0);
});

test('Binance вырос, HL выше → шорт-сигнал, но Binance против → ЖДАТЬ (veto)', () => {
  const m = build({ bfNow: 83485, hlNow: 83502 });   // spread = +17 → dev ≈ +19 ≥ 12 → рывок; Binance упал на 15
  const r = m.evaluate('BTC', T0, ok);
  assert.equal(r.side, 'S');
  assert.equal(r.rec, 'wait');
  assert.ok(r.title.startsWith('ЖДАТЬ'));
});

test('очередь не проходит / пауза / нет свежих цен', () => {
  const m = build({ bfNow: 83509, hlNow: 83502 });
  assert.equal(m.evaluate('BTC', T0, { ...ok, timingAllow: false, timingReasons: ['x'] }).rec, 'wait');
  assert.equal(m.evaluate('BTC', T0, { ...ok, paused: true }).rec, 'wait');
  assert.equal(m.evaluate('BTC', T0, { ...ok, timingAllow: null }).rec, 'long', 'нет оценки очереди — не блокирует');
  const stale = m.evaluate('BTC', T0 + 10000, ok);
  assert.equal(stale.stale, true); assert.equal(stale.rec, 'wait');
});

test('мало данных: фон ещё считается', () => {
  const m = S.createModel();
  m.tick('f', 'BTC', T0 - 100, 83000); m.sample('BTC', T0 - 50, 83001);
  const r = m.evaluate('BTC', T0, ok);
  assert.equal(r.ready, false); assert.ok(/фон/.test(r.title));
});

test('нет сигнала при малом отклонении', () => {
  const m = build({ bfNow: 83500.5, hlNow: 83502 });
  const r = m.evaluate('BTC', T0, ok);
  assert.equal(r.rec, 'none');
});
