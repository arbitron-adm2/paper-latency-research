'use strict';
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const { createRequire } = require('node:module');
const { DatabaseSync } = require('node:sqlite');
const { WalletView } = require('../../apps/vchera2/execution-state.cjs');
const { createCloseSender } = require('../../apps/vchera2/close-request.cjs');
const baseConfig = require('../../apps/vchera2/launch-config.json');
const filename = path.resolve(__dirname, '../../apps/vchera2/scanner/btcy2-lag.js');
const realRequire = createRequire(filename);
const source = fs.readFileSync(filename, 'utf8') + '\nmodule.exports.testing={positions,pending,closePx,openPosition,closePosition,liveTick,adoptOrphans,orphans,livePnlSync,executionSync,feed,guardTick,cfg,queueEntry,liveEntry,cancelledLive,mainStrategy,y2Strategy,liveStrategies,mainStrategyTick};';
function makeHarness(sharedDb = null, { main = false, y2 = false, ignoreFunding = false, session = null } = {}) {
  const db = sharedDb || new DatabaseSync(':memory:');
  const config = structuredClone(baseConfig);
  // This fixture exercises the original 2100x600 executor, independently of the
  // currently selected local live probe/candidate and its private warmup file.
  delete config.candidate;
  delete config.independentGuards.liveSession;
  if (session) config.independentGuards.liveSession = session;
  if (main) config.mainStrategy = { ...config.mainStrategy, enabled: true };
  if (!main) delete config.mainStrategy;
  if (!y2) delete config.y2Strategy;
  else {const r=require('../../apps/vchera2/y2-live-runtime.cjs');config.y2Strategy={enabled:true,id:r.ID,profileHash:r.PROFILE_HASH,sharedSlot:true};}
  config.strategy.margin = 2100; config.strategy.leverage = 600;
  config.strategy.live = true; config.strategy.enabled = true;
  if(main||y2){config.strategy.margin=100;config.strategy.leverage=500;}
  const view = new WalletView(config.wallet);
  view.apply({ address: config.wallet, balance: '2300000000000000000000', funds: { availableBalance: '2300000000000000000000' }, openPositions: [], operations: [], sessionKeys: [] }, { initial: true });
  const calls = [], listeners = [], responses = [];
  let history = [], nonce = 0;
  const account = {
    wallet: view.wallet, keyStatus: () => ({active:true}),
    onTrade: fn => listeners.push(fn), onQueueSample: () => {}, queueSamples: () => [], queueRank: async()=>({}),
    confirmClosed: (id,ev) => view.confirmClosed(id,ev), cancel: async()=>({ok:false,status:409,data:{code:'intent_conflict'}}),
    open: async()=>{throw new Error('Tests must not send entries');},
    status: () => ({key:{active:true},wallet:view.wallet,tradesConnected:true}),
  };
  account.close = createCloseSender({ master: config.wallet, takeNonce:()=>String(++nonce), deadline:()=>String(Math.floor(Date.now()/1000)+3600),
    sign:async message=>({intentId:'0x'+Buffer.from(JSON.stringify(message)).toString('hex'),signed:message}),
    post:async body=>{calls.push(structuredClone(body));return responses.shift() || {ok:true,status:201,data:{}};} });
  const manualStops=[], runtimeFiles=new Map();
  const safety = { config, lockedStrategyConfig: defaults=>({...defaults,...config.strategy,ptWallet:config.wallet}),
    setManualStop:value=>manualStops.push(value),highWaterTrippedToday:()=>false,
    rejectUnsafePatch:p=>{for(const [k,v]of Object.entries(p))if(!['live','enabled'].includes(k)||v!==false)throw new Error('Unsafe patch');},
    cutoffReached:()=>false, tradingWindowReason:()=>null, walls:{reason:()=>null,update:()=>{}}, highWater:()=>({tripped:false,best:0,drawdown:0}) };
  const sandbox = { module:{exports:{}}, exports:{}, __filename:filename, __dirname:path.dirname(filename),
    require:id=>id==='./../db'?{db}:id==='./papertrade'?{getAccount:()=>account}:id==='../safety.cjs'?safety:id==='../runtime.cjs'?{privateRuntime:()=>'/fixture',atomicPrivateJSON:(p,s)=>runtimeFiles.set(p,structuredClone(s))}:id==='./funding-guard'&&ignoreFunding?{...realRequire(id),noEntry:()=>null}:realRequire(id),
    console:{log:()=>{},error:()=>{}}, WebSocket:class{}, AbortSignal, Buffer,
    setInterval:()=>1,clearInterval:()=>{},setTimeout:()=>1,clearTimeout:()=>{},
    fetch:async()=>({ok:true,status:200,json:async()=>({items:structuredClone(history)})}),
  };
  sandbox.global=sandbox;
  vm.runInNewContext(source,sandbox,{filename});
  const api=sandbox.module.exports;
  const test=api.testing;
  test.feed.hl.bid=81583.5;test.feed.hl.ask=81583.5;test.feed.bf.bid=81583.5;test.feed.bf.ask=81583.5;test.feed.hl.t=Date.now();test.feed.bf.t=Date.now();
  return {db,view,account,calls,responses,api,test,config,safety,manualStops,setHistory:x=>{history=x;},emit:t=>listeners.forEach(fn=>fn(t))};
}
function fill(id='42') { return {positionId:id,intentId:'0xown',entry:81583.5,margin:2100,leverage:600,bust:81681.5,openedAt:Date.now()-1000}; }
function tuple(id='42') { return [id,0,false,0,'815835','2100000000000000000000',600,'816815',Date.now()-1000,'0xown']; }
function open(h,id='42') { return h.test.openPosition('book','S',{books:[],dev:1.2,binMove:0,fill:fill(id)}); }
function evidence(px=81586.5) { return {px,ts:Date.now(),source:'test-public-trades'}; }
function closeTrade(id='42') { return {kind:'PositionClosed',positionId:id,exitPriceRaw:'815865',priceScaleRaw:'10',timestampMs:Date.now(),adjustedPnlRaw:'-46332898196326463071',userPaidFeeRaw:'0'}; }
module.exports={makeHarness,fill,tuple,open,evidence,closeTrade};
