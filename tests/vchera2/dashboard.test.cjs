'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const vm = require('node:vm');
const fs = require('node:fs');
const script = fs.readFileSync(require.resolve('../../apps/vchera2/dashboard.js'), 'utf8');
function fixture() {
  return {
    observedAt: new Date().toISOString(), LIVE: false, liveExecutionIntegrated: false, liveReady: false, specReady: false,
    fullMarginReady: true, marketFresh: true, providerFresh: true, technicalReady: true, entriesEnabled: false,
    authorizationReady: false, exchangeMutationsEnabled: false, pid: 12345, actualWallet: '0x1111111111111111111111111111111111111111',
    maximumAuthorizedNotionalUsd: 1260000,
    config: { margin: 2100, leverage: 600, maxPositions: 1, noLongs: false, stopUsd: 4, cushionExitUsd: 4, cushionExitMs: 1000, binExitUsd: 8, entryMaxWaitMs: 15000, roundZoneUsd: 50, roundZoneAboveUsd: 100, maxFeedAgeMs: 1500 },
    provider: { wallet: { available: 2100.251514, balance: 2100.251514, locked: 0, openPositions: [], operations: [] } },
    blockers: [{ code: 'STOP_TIME_TIMEZONE_UNRESOLVED' }],
    feeds: { hl: { mid: 81520.5, ageMs: 100, bookAgeMs: 500, imb: -0.6 }, bf: { mid: 81496.45, ageMs: 10, bookAgeMs: 100, imb: 0.2 } },
    highWater: { armed: false }, timer: { armed: false, absoluteCutoff: null }, funding: { noEntryReason: null },
    regime: { regime: 'NORMAL' }, walls: { equivalenceVerified: false, blockL: null, blockS: null }, decisions: {},
  };
}
function harness(response) {
  const elements = new Map(), requests = [], timers = [], listeners = new Map();
  const node = () => ({ textContent: '', className: '', disabled: false, checked: false, dataset: {}, children: [], classList: { add() {} },
    append(...a) { this.children.push(...a); }, prepend(...a) { this.children.unshift(...a); }, replaceChildren(...a) { this.children = a; },
    addEventListener(name, fn) { this[name] = fn; }, setAttribute() {} });
  const document = { hidden: false, getElementById(id) { if (!elements.has(id)) elements.set(id,node()); return elements.get(id); }, createElement: node, addEventListener(name,fn) { listeners.set(name,fn); } };
  let mode = response;
  let controlResult={ok:true,message:'Команда принята'},controlStatus=200;
  const context = vm.createContext({ document, Date, performance, AbortSignal, window: { addEventListener() {},confirm:()=>true },
    setInterval(fn,ms) { timers.push({fn,ms}); }, setTimeout() { return 1; }, clearTimeout() {},
    async fetch(url, options) { requests.push({url,options}); if(mode instanceof Error)throw mode;if(options.method==='POST')return{ok:controlStatus<400,json:async()=>controlResult};return {ok:true,json:async()=>mode}; } });
  vm.runInContext(script+'\nglobalThis.dashboardTest={poll,tick,tradingControl};',context);
  return { elements, requests, timers, control:a=>context.dashboardTest.tradingControl(a),setControlResult:(r,status)=>{controlResult=r;controlStatus=status;}, poll:()=>context.dashboardTest.poll(), setResponse(v) { mode=v; }, settle: () => new Promise(resolve=>setImmediate(resolve)) };
}
test('dashboard shows funded margin, LIVE off and missing executor without claiming trade activity', async () => {
  const h=harness(fixture()); await h.settle();
  assert.match(h.elements.get('available').textContent,/USDC/);
  assert.match(h.elements.get('margin-note').textContent,/Полная маржа с резервом доступна/);
  assert.match(h.elements.get('live-label').textContent,/LIVE выключен/);
  assert.equal(h.elements.get('positions').textContent,'0 / 0');
  assert.equal(h.elements.get('blockers').children.length,2);
  assert.match(h.elements.get('blockers').children[1].children[0].textContent,/Торговый транспорт/);
  assert.equal(h.requests[0].url,'/state');
  assert.equal(h.requests[0].options.method,undefined);
  assert.equal(h.timers[0].ms,100);
});
test('dashboard marks lost connection as UNKNOWN, never retains stale LIVE assertion', async () => {
  const h=harness(fixture()); await h.settle(); h.setResponse(new Error('offline'));
  await h.poll(); await h.settle();
  assert.match(h.elements.get('live-label').textContent,/неизвестен/);
  assert.match(h.elements.get('mode-description').textContent,/предыдущий снимок/);
  assert.equal(h.elements.get('connection').className,'pill bad');
});
test('dashboard rejects stale snapshots and emits no exchange mutations', async () => {
  const s=fixture();s.observedAt=new Date(Date.now()-60000).toISOString();
  const h=harness(s);await h.settle();
  assert.equal(h.elements.get('connection').className,'pill bad');
  assert.match(h.elements.get('live-label').textContent,/неизвестен/);
  assert.ok(h.requests.every(r=>r.url==='/state' && r.options.method===undefined));
});
test('dashboard treats provider strings as text and exposes only explicit start/stop controls', async () => {
  const s=fixture();s.blockers=[{code:'TEST',detail:'<img src=x onerror=alert(1)>'}];
  const h=harness(s);await h.settle();
  assert.equal(h.elements.get('blockers').children[0].children[1].textContent,s.blockers[0].detail);
  assert.equal(script.includes('innerHTML'),false);
  const html=fs.readFileSync(require.resolve('../../apps/vchera2/dashboard.html'),'utf8');
  assert.equal((html.match(/<button\b/g)||[]).length,6);
  assert.ok(html.includes('id="pause"'));
  assert.ok(html.includes('id="start-trading"'));assert.ok(html.includes('id="stop-trading"'));
});
test('Paper quote is distinct, spread is Paper minus Binance and separate book is N/A',async()=>{
  const s=fixture();s.feeds.paper={mid:81550.7,bid:81550.5,ask:81550.9,ageMs:20,bookAgeMs:null,bookNA:true};
  const h=harness(s);await h.settle();
  assert.match(h.elements.get('paper-spread').textContent,/54,25/);
  assert.match(h.elements.get('feeds').children[0].children[0].textContent,/Paper/);
  assert.equal(h.elements.get('feeds').children[0].children[5].textContent,'нет отдельного стакана');
});
test('GUI stop/start send only explicit same-origin POST with required control header',async()=>{
  const s=fixture();s.liveExecutionIntegrated=true;
  const h=harness(s);await h.settle();await h.control('stop');await h.control('start');
  const posts=h.requests.filter(r=>r.options.method==='POST');assert.equal(posts.length,2);
  assert.equal(posts[0].url,'/api/trading/stop');assert.equal(posts[1].url,'/api/trading/start');
  assert.equal(posts[0].options.headers['X-Vchera2-Control'],'1');
  assert.match(h.elements.get('control-message').textContent,/Команда принята/);
});
test('GUI displays rejected start without claiming success',async()=>{
  const s=fixture();s.liveExecutionIntegrated=true;
  const h=harness(s);await h.settle();h.setControlResult({ok:false,reasons:['Время торговли вышло']},409);await h.control('start');
  assert.match(h.elements.get('control-message').textContent,/Время торговли вышло/);assert.match(h.elements.get('control-message').className,/bad/);
});
test('pending guard/shadow disables Start and even direct UI handler sends no start request',async()=>{
  const s=fixture();s.liveExecutionIntegrated=true;s.instrumentSelection={liveReady:false,reason:'Guard/shadow пока не подключены'};
  const h=harness(s);await h.settle();assert.equal(h.elements.get('start-trading').disabled,true);await h.control('start');
  assert.equal(h.requests.filter(r=>r.options.method==='POST').length,0);assert.match(h.elements.get('control-message').textContent,/Guard\/shadow/);
});
test('wallet ETH position uses ETH mark and unknown instrument is never priced with BTC',async()=>{
  const s=fixture();s.provider.wallet.openPositions=[{positionId:'eth-1',instrumentId:1,isLong:false,entry:3301,margin:10,leverage:2,openedAt:Date.now(),bust:4000},{positionId:'unknown',instrumentId:99,isLong:true,entry:5,margin:10,leverage:2}];
  s.markets={ETH:{maxFeedAgeMs:1500,feeds:{hl:{mid:3300.25,ageMs:100}}}};
  const h=harness(s);await h.settle();const rows=h.elements.get('open').children;
  assert.match(rows[0].children[0].textContent,/ETH/);assert.match(rows[0].children[4].textContent,/3.*300/);assert.match(rows[0].children[5].textContent,/0,75/);
  assert.equal(rows[1].children[4].textContent,'—');assert.match(rows[1].children[0].textContent,/Неизвестный тикер/);
});
test('realised ledger day PnL overrides reset zero from stopped legacy guard',async()=>{
 const s=fixture();s.guard={dayPnl:0};s.pnlDay={realisedUsd:-334.965834};const h=harness(s);await h.settle();assert.match(h.elements.get('day-pnl').textContent,/334,97/);assert.equal(h.elements.get('day-pnl').className,'neg');
});
