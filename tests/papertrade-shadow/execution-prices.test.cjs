'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const {DatabaseSync}=require('node:sqlite');
const {ExecutionPrices,POLICY}=require('../../apps/papertrade-shadow/execution-prices.cjs');
const {Execution}=require('../../apps/papertrade-shadow/engine.cjs');
const {createNativeBridge}=require('../../apps/papertrade-shadow/native-bridge.cjs');
const {buildProfile}=require('../../apps/papertrade-shadow/persistent-profile.cjs');
const {PersistentObserver}=require('../../apps/papertrade-shadow/persistent-session.cjs');
const {captureEngine,restoreEngine,resumeCensored}=require('../../apps/papertrade-shadow/engine-state.cjs');
const fixture=require('./fixtures/first-main-live-20261021.json');
const T=fixture.live.decisionMs,UNIT=10n**18n;
let sequence=0;
function quote(at,bid,ask=bid+1,extra={}){return{kind:'market',symbol:'BTC',venue:'hl',sequence:++sequence,recv_ms:at,source_ms:at,payload:{frameKind:'quote',bid:String(bid),ask:String(ask)},...extra};}
function make(at=T){
 const p=buildProfile(),db=new DatabaseSync(':memory:'),config={...p.config,entryDeadline:{signalTtlMs:20000,safetySeconds:1},persistentService:{enabled:false}},experiment={margin:100,leverage:500,balance:'2300'};
 const scenario={...p.scenario,entryMs:[4156],exitMs:[0],cancelMs:[10],congestion:0,volatility:0,cancelConflictProbability:1,nonstop:true,fillFallbackMaxAgeMs:30000};
 const e=new Execution(config,experiment,scenario,at);e.bridge=createNativeBridge(e,{asset:'BTC',namespace:'sim_native_price_test'},db,at);e.inputHealthy=true;e.integrityWarmUntil=at;
 return{e,db,close:()=>db.close()};
}
function submit(e){return e.submit({strategy:'spread',side:'L',margin:100,ctx:{sig:9,dev:-9,spread:-2.6,baseline:6.4,books:[]}});}
function due(e,at){while(e.timers[0]?.at<=at){const t=e.timers.shift();e.mark(Math.max(t.at,e.now));e.fire(t);}e.mark(at);}
test('synthetic changed-price fixture: execution-time quotes reproduce the expected loss at 100x500',()=>{
 const x=make();try{
  const {e}=x;e.executionPrices.stage(fixture.quotes,fixture.live.exitMs);const id=submit(e);assert.ok(id);
  due(e,fixture.live.entryMs);const p=[...e.positions.values()][0];assert.equal(p.entry,831695n);
  e.mark(fixture.live.exitMs);e.finish(p,e.quote(),'fixed synthetic exit time',false,{priceEvidence:e.executionQuote(e.now)});
  assert.ok(Math.abs(Number(e.realised)/1e18-fixture.live.realPnl)<.000001);
  assert.equal(e.orders[0].exit_raw,'831605');assert.equal(e.orders[0].entryPriceEvidence.policy,POLICY);
  assert.ok(e.orders[0].entryPriceEvidence.ageMs<=1500);assert.ok(e.orders[0].exitPriceEvidence.ageMs<=1500);
  assert.equal(e.ledger.some(r=>r.kind==='fill_price_proxy'),false);
 }finally{x.close();}
});
test('old shadow execution times also use 83169.5, never the old decision price 83114.5',()=>{
 const prices=new ExecutionPrices();prices.stage(fixture.quotes,fixture.oldShadow.exitMs);
 for(const [at,expected]of[[fixture.oldShadow.entryMs,83169.5],[fixture.oldShadow.exitMs,83160.5]]){const q=prices.at(at);assert.ok(q);assert.equal((q.bid+q.ask)/2,expected);assert.ok(q.recvMs<=at);}
});
test('staged future price, book depth, other assets and stale source timestamps cannot fabricate an execution quote',()=>{
 const p=new ExecutionPrices();p.observe(quote(T,83114));p.observe(quote(T+3000,84000));assert.equal(p.at(T+2000),null,'future quote excluded; old one stale');
 const q=new ExecutionPrices();q.observe(quote(T,83114,83115,{source_ms:T-2000}));assert.equal(q.at(T),null,'source age checked');
 const b=new ExecutionPrices();b.observe(quote(T,83114,83115,{payload:{frameKind:'book',bid:'83114',ask:'83115'}}));b.observe(quote(T,3000,3001,{symbol:'ETH'}));b.observe(quote(T,83114,83115,{venue:'bf'}));assert.equal(b.at(T),null);
});
test('strict (not non-stop) missing entry price retains margin and unknown outcome across restart, without PnL or release on a later quote',()=>{
 const x=make(),y=make(T+5000);try{
  x.e.scenario.nonstop=false;y.e.scenario.nonstop=false;
  const {e}=x;e.executionPrices.observe(quote(T,83114));const id=submit(e);due(e,T+4200);
  assert.equal(e.intents.get(id).state,'outcome_unknown');assert.equal(e.reserved,100n*UNIT);assert.equal(e.realised,0n);assert.equal(e.counts.filled,0);
  restoreEngine(y.e,captureEngine(e));resumeCensored(y.e,T+5000);y.e.executionPrices.observe(quote(T+5000,83169));due(y.e,T+5200);
  assert.equal(y.e.intents.get(id).state,'outcome_unknown');assert.equal(y.e.reserved,100n*UNIT);assert.equal(y.e.counts.filled,0);assert.equal(y.e.realised,0n);
 }finally{x.close();y.close();}
});
test('non-stop missing entry price releases the reserve at once: no fill, no PnL, no fill from a later quote',()=>{
 const x=make();try{
  const {e}=x;e.executionPrices.observe(quote(T,83114));const id=submit(e);due(e,T+4200);
  assert.equal(e.intents.get(id).state,'expired');assert.equal(e.reserved,0n);assert.equal(e.realised,0n);assert.equal(e.counts.filled,0);assert.equal(e.counts.executionPriceUnknown,1);
  assert.ok(e.ledger.some(r=>r.kind==='entry_price_unavailable_released'&&r.executionAtMs===T+4156));
  e.executionPrices.observe(quote(T+5000,83169));due(e,T+5200);assert.equal(e.counts.filled,0);assert.equal(e.positions.size,0);
 }finally{x.close();}
});
test('failed cancellation retains the actual changed execution price, even with unhealthy strategy input',()=>{
 const x=make();try{
  const {e}=x;e.executionPrices.stage(fixture.quotes,fixture.live.entryMs);const id=submit(e);e.mark(T+1000);e.cancel(id,'signal gone');
  assert.ok(e.timers.every(t=>Number.isFinite(t.at)),'missing strategy quote cannot create a NaN cancellation deadline');
  e.inputHealthy=false;due(e,fixture.live.entryMs);
  assert.equal(e.counts.cancelConflict,1);assert.equal(e.counts.filled,1);assert.equal(e.counts.lateFill,1);assert.equal([...e.positions.values()][0].entry,831695n);assert.equal(e.fallbackQuote(),null);
 }finally{x.close();}
});
function event(seq,at,bid){const e=quote(at,bid);e.sequence=seq;Object.assign(e,{schemaVersion:1,provenance:'synthetic batched worker regression, not performance'});Object.assign(e.payload,{instrumentId:0,priceScale:'10',sourceTimestampKnown:true});e.payload_sha256=require('node:crypto').createHash('sha256').update(JSON.stringify(e.payload)).digest('hex');return e;}
function envelope(events,now,last=events.at(-1)?.sequence??0){return{schemaVersion:1,mode:'READ_ONLY_RESEARCH_NO_ORDERS',bootId:'prices-test',observedAtMs:now,firstSequence:1,lastSequence:last,gap:false,events};}
for(const paginated of[false,true])test('worker batch '+(paginated?'pagination holds timers until the quote is consumed':'stages fresh execution prices before overdue timers fire'),()=>{
 const parent=fs.mkdtempSync(path.join(path.resolve(__dirname,'../../reports'),'price-regression-'));let o;
 try{
  o=new PersistentObserver({stateDir:path.join(parent,'state'),clock:()=>T,comparisonEnabled:true});o.batch(envelope([],T),T);
  o.batch(envelope([event(1,T,83114)],T),T);
  const m=o.suite.models.find(m=>m.id==='main_btc_observe'),e=m.engine;e.inputHealthy=true;e.integrityWarmUntil=T;e.entryGate=()=>({allowed:true});Object.assign(e.scenario,{entryMs:[2000],congestion:0,volatility:0});
  assert.ok(e.submit({strategy:'spread',side:'L',margin:2100,ctx:{sig:9,dev:-9,spread:-2.6,baseline:6.4,books:[]}}));
  const a=event(2,T+100,83114),b=event(3,T+1900,83169);
  if(paginated){o.batch(envelope([a],T+5000,3),T+5000);o.heartbeat(T+5000);assert.equal(e.counts.filled,0);assert.equal(e.now,T+100);o.batch(envelope([b],T+5000,3),T+5000);}
  else o.batch(envelope([a,b],T+5000),T+5000);
  o.heartbeat(T+5000);
  assert.equal(e.counts.filled,1,JSON.stringify({counts:e.counts,error:m.lastError,ledger:e.ledger},(_,v)=>typeof v==='bigint'?String(v):v));assert.equal([...e.positions.values()][0].entry,831695n);
  assert.equal(e.ledger.some(r=>r.kind==='fill_price_proxy'),false);assert.equal(e.counts.executionPriceUnknown??0,0);
 }finally{if(o&&!o.history.closed)o.stop('UNIT_END',o.session.now);fs.rmSync(parent,{recursive:true,force:true});}
});
