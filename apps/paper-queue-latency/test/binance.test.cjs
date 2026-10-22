'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const B = require('../src/binance.js');
const hook = require('../src/chart-hook.js');

const spot = '{"stream":"btcusdt@trade","data":{"e":"trade","E":1,"s":"BTCUSDT","t":1,"p":"83012.00000000","q":"0.00011000","T":1792536300222,"m":true}}';
const fut = '{"stream":"ethusdt@aggTrade","data":{"e":"aggTrade","E":1,"a":1,"s":"ETHUSDT","p":"2456.73","q":"0.002","T":1792536309177,"m":true}}';

test('parse: spot trade и futures aggTrade → тик рынка, остальное отбрасывается', () => {
  assert.deepEqual(B.parse('s', spot), { v: 's', m: 'BTC', p: 83012, q: 0.00011, ev: 1792536300222 });
  assert.deepEqual(B.parse('f', fut), { v: 'f', m: 'ETH', p: 2456.73, q: 0.002, ev: 1792536309177 });
  assert.equal(B.parse('s', '{"data":{"e":"trade","s":"SOLUSDT","p":"1","q":"1"}}'), null);
  assert.equal(B.parse('s', '{"data":{"e":"trade","s":"BTCUSDT","p":"0","q":"1"}}'), null);
  assert.equal(B.parse('s', 'не json'), null);
  assert.equal(B.parse('s', '{"result":null,"id":1}'), null);
});

test('store: ряды по "venue:market", обрезка по возрасту и размеру', () => {
  const st = B.createStore();
  for (let i = 0; i < 100; i++) st.add('s', 'BTC', i * 1000, 100 + i);
  st.add('f', 'BTC', 5, 1);
  const d = st.dump();
  assert.deepEqual(Object.keys(d).sort(), ['f:BTC', 's:BTC']);
  assert.ok(d['s:BTC'][0][0] >= 99000 - B.KEEP_MS && d['s:BTC'].length === 100);
  for (let i = 0; i < 300; i++) st.add('s', 'BTC', 400000 + i, 1);
  assert.ok(st.dump()['s:BTC'][0][0] >= 400299 - B.KEEP_MS);
  assert.deepEqual(st.last('f', 'BTC'), [5, 1]);
  st.clear(); assert.deepEqual(st.dump(), {});
});

test('feed: стартует два сокета, тики идут с временем приёма, stop закрывает и не переподключает', () => {
  const made = [], timers = [];
  class WS { constructor(u) { this.u = u; made.push(this); } close() { this.closed = true; } }
  const got = [], states = [];
  let now = 1000;
  const f = B.createFeed({ WS, now: () => now, setT: (fn, ms) => { timers.push([fn, ms]); return timers.length; }, clearT: () => {},
    onTick: (v, m, t, p) => got.push([v, m, t, p]), onState: (v, s) => states.push(v + ':' + s) });
  f.start();
  assert.equal(made.length, 2); assert.ok(made[0].u.includes('stream.binance.com') && made[1].u.includes('fstream.binance.com/market/stream'));
  made[0].onopen(); now = 1500; made[0].onmessage({ data: spot }); made[1].onmessage({ data: fut });
  assert.deepEqual(got, [['s', 'BTC', 1500, 83012], ['f', 'ETH', 1500, 2456.73]]);
  made[0].onclose();  // обрыв → ожидание повторного подключения
  assert.ok(states.includes('s:error'));
  f.stop(); assert.ok(made[1].closed); assert.ok(states.includes('f:off'));
  for (const [fn] of timers.splice(0)) fn();
  assert.equal(made.length, 2, 'после stop новых сокетов нет');
});

test('chart-hook: подписи осей → шкалы цены и времени', () => {
  const now = new Date(2026, 9, 21, 12, 0, 30).getTime();
  const labels = [];
  // цена: y = 400 - 0.005*(p-80000): 83000 → 385, 83100 → 384.5 … берём шаг 200 с нужными пикселями
  const Y = (p) => 400 - 0.2 * (p - 83000);
  for (const p of [83000, 83050, 83100, 83150]) labels.push({ s: p.toLocaleString('en-US'), x: 935, y: Y(p), al: 'left', bl: 'middle' });
  labels.push({ s: 'LIQ', x: 400, y: 20, al: 'center', bl: 'middle' });     // не число
  const X = (t) => 900 + 0.01 * (t - now);                                   // 10 px на секунду
  for (const dt of [-40000, -30000, -20000, -10000, 0]) {
    const d = new Date(now + dt); const hh = String(d.getHours()).padStart(2, '0'), mm = String(d.getMinutes()).padStart(2, '0'), ss = String(d.getSeconds()).padStart(2, '0');
    labels.push({ s: `${hh}:${mm}:${ss}`, x: X(now + dt), y: 880, al: 'center', bl: 'alphabetic' });
  }
  const a = hook.axes(labels, now);
  assert.ok(a);
  assert.equal(a.cw, 930);
  assert.ok(Math.abs(a.y0 + a.yk * 83075 - Y(83075)) < 1e-6);
  assert.ok(Math.abs(a.x0 + a.xk * (now - 5000) - X(now - 5000)) < 1e-4);
  assert.equal(hook.axes(labels.filter((l) => l.al !== 'center'), now), null, 'без оси времени шкалы нет');
});

test('chart-hook: час через полночь и мусор в подписях', () => {
  const now = new Date(2026, 9, 21, 0, 0, 10).getTime();
  const t = hook.parseClock('23:59:50', now);
  assert.equal(t, new Date(2026, 9, 20, 23, 59, 50).getTime());
  assert.equal(hook.parseClock('Oct 21', now), null);
  assert.equal(hook.parseClock('25:00', now), null);
  assert.equal(hook.parsePrice('$83,012.5'), 83012.5);
  assert.equal(hook.parsePrice('83K'), null);
  // подпись, выбивающаяся из прямой, отбраковывается; две точки с большим отклонением — нет шкалы
  const pts = [[0, 0], [1, 10], [2, 20], [3, 30], [4, 100]];
  const f = hook.fit(pts); assert.ok(f && Math.abs(f.b - 10) < 1e-9);
  assert.equal(hook.fit([[0, 0], [1, 10], [2, 33]]), null);
});
