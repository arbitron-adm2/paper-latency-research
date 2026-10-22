'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const {DatabaseSync}=require('node:sqlite');
const {MainBtcRuntime,ID:MAIN_ID}=require('../../apps/vchera2/main-btc-runtime.cjs');
const {Y2LiveRuntime,ID:Y2_ID,PROFILE_HASH}=require('../../apps/vchera2/y2-live-runtime.cjs');
const {makeHarness}=require('./execution-harness.cjs');
const {MarketData}=require('../../apps/vchera2/market-data.cjs');
function fixture(){
 let now=Date.parse('2026-10-21T09:10:00Z');const db=new DatabaseSync(':memory:'),marketData=new MarketData({now:()=>now});
 db.exec('CREATE TABLE btcy2_lag_trades(strategy TEXT,live INTEGER,status TEXT,qty REAL,real_pnl REAL)');
 const options={db,marketData,now:()=>now};
 const main=new MainBtcRuntime(options),y2=new Y2LiveRuntime(options),provider={wallet:{available:2300,t:now,operations:[],openPositions:[]},entrySafety:{unknown:[]}};
 function feed(px=100000){for(const venue of ['hl','bf','bs']){const price=venue==='hl'?px:100000;marketData.quote('BTC',venue,price-.5,price+.5,now);marketData.publish('BTC',venue,'quote',now);marketData.book('BTC',venue,[[price-.5,1]],[[price+.5,1]],now);marketData.publish('BTC',venue,'book',now);}}
 function order(side='L'){return{intentId:'0x'+(side==='L'?'a':'b').repeat(64),side,margin:100,leverage:500,createdAt:now,validUntilMs:now+20000,ctx:{nativeRequestId:'local-'+side,nativeStrategy:'spread',hl:100000,sig:4,dev:-4,spread:0,baseline:4,binMove:4,books:[]}};}
 return{db,main,y2,provider,options,feed,order,now:()=>now,advance:ms=>{now+=ms;provider.wallet.t=now;}};
}
for(const side of['L','S'])test('Y2 '+side+' confirmed fill, native stop and close use isolated journal and preserve main runtime',()=>{
 const f=fixture();try{
  const q=f.order(side);f.feed();f.y2.remember(q);const p={id:1,strategy:Y2_ID,live:true,side,positionId:'42',intentId:q.intentId,entry:100000,qty:.5,margin:100,leverage:500,opened_at:f.now()};
  const state={positions:new Map([[1,p]]),pending:[],provider:f.provider};f.y2.sync(state);f.main.sync(state);
  assert.equal(f.y2.bridge.api.positions.size,1);assert.equal(f.main.bridge.api.positions.size,0);
  f.advance(100);f.feed(side==='L'?99995:100005);f.y2.advance(f.now(),false);
  assert.equal(f.y2.closes.length,1);assert.match(f.y2.closes[0].why,/стоп/);assert.equal(f.y2.bridge.api.positions.size,1);
  f.y2.closed(p,{px:side==='L'?99994:100006,ts:f.now()+5000,realPnl:-3});
  assert.equal(f.y2.bridge.api.positions.size,0);assert.equal(f.db.prepare('SELECT count(*) n FROM sim_native_y2_live_trades').get().n,1);assert.equal(f.db.prepare('SELECT count(*) n FROM sim_native_main_btc_live_trades').get().n,0);
 }finally{f.db.close();}
});
test('Y2 unknown intent recovers only its own identity, exact side and 15 s cancel window; checkpoint keeps source state',()=>{
 const f=fixture();try{
  const q=f.order('S');f.y2.remember(q);f.provider.entrySafety.unknown=[{intentId:q.intentId}];const state={pending:[],positions:new Map(),cancelledLive:new Map(),provider:f.provider};f.y2.recover(state);f.main.recover(state);f.y2.recover(state);
  assert.equal(state.pending.length,1);assert.equal(state.pending[0].strategy,Y2_ID);assert.equal(state.pending[0].side,'S');assert.equal(state.pending[0].entryMaxWaitMs,15000);
  const p={id:1,strategy:Y2_ID,live:true,side:'S',positionId:'42',intentId:q.intentId,entry:100000,qty:.5,margin:100,leverage:500,opened_at:f.now()};f.feed();f.y2.sync({positions:new Map([[1,p]]),pending:[],provider:f.provider});const native=[...f.y2.bridge.api.positions.values()][0];native.maxFav=12;native.bBest=-99970;f.y2.save();
  const again=new Y2LiveRuntime(f.options);again.sync({positions:new Map([[1,p]]),pending:[],provider:f.provider});assert.equal([...again.bridge.api.positions.values()][0].maxFav,12);assert.equal([...again.bridge.api.positions.values()][0].bBest,-99970);assert.equal(f.db.prepare('SELECT count(*) n FROM sim_native_y2_live_trades').get().n,1);
 }finally{f.db.close();}
});
test('two simultaneous signals reserve one global slot before signing; a queued close also blocks the other strategy',async()=>{
 const h=makeHarness(null,{main:true,y2:true,ignoreFunding:true});try{
  const t=Date.now(),submitted=[];h.test.executionSync.ready=true;h.test.executionSync.t=t;
  h.account.open=async(args,hooks)=>{const intentId='0x'+'c'.repeat(64);hooks.onPrepared({intentId,deadline:String(Math.floor(t/1000)+18)});hooks.beforeSend();submitted.push(args);return{ok:true,status:201,intentId};};
  const enabled=[];for(const r of h.test.liveStrategies){r.blockers=()=>[];r.advance=(at,allowed)=>{enabled.push([r.id,allowed]);if(allowed)r.requests.push({requestId:'native-'+r.id,createdAt:at,strategy:'spread',side:'L',ctx:{books:[],sig:4,dev:-4,spread:0,baseline:4,binMove:4}});};}
  for(const r of h.test.liveStrategies)h.test.mainStrategyTick(t,r);
  await new Promise(r=>setImmediate(r));assert.equal(submitted.length,1);assert.equal(h.test.pending.length,1);assert.equal(submitted[0].margin,100);assert.equal(submitted[0].leverage,500);assert.deepEqual(enabled.map(x=>x[1]),[true,false]);
  h.test.pending.length=0;h.test.positions.set(999,{strategy:MAIN_ID,live:true,closing:{intentId:'close-in-queue'}});h.test.mainStrategyTick(t,h.test.y2Strategy);assert.equal(submitted.length,1);assert.equal(enabled.at(-1)[1],false);
 }finally{for(const r of h.test.liveStrategies)r.detach();h.db.close();}
});
test('late Y2 fill requests protective close and manual STOP prevents both strategies from sending',async()=>{
 const h=makeHarness(null,{main:true,y2:true});try{
  const p=h.test.openPosition(Y2_ID,'L',{books:[],fill:{positionId:'42',intentId:'0x'+'a'.repeat(64),entry:100000,margin:100,leverage:500,openedAt:Date.now(),invalidatedReason:'signal gone'}});assert.ok(p.closing);
  h.test.cfg.live=false;assert.equal(h.test.liveEntry(Y2_ID,'L',{hl:100000,signalAtMs:Date.now(),validUntilMs:Date.now()+20000,books:[]},100),null);
  await new Promise(r=>setImmediate(r));
 }finally{for(const r of h.test.liveStrategies)r.detach();h.db.close();}
});
test('dual approval binds both exact profiles and the single 50k exposure limit',()=>{
 const safety=require('../../apps/vchera2/safety.cjs'),c=structuredClone(safety.config);c.mainStrategy={...c.mainStrategy,enabled:true};c.y2Strategy={enabled:true,id:Y2_ID,profileHash:PROFILE_HASH,sharedSlot:true};c.liveTestApproval.y2Strategy={approved:true,profileHash:PROFILE_HASH,margin:100,leverage:500};assert.ok(safety.assertLaunchConfig(c));
 for(const mutate of[x=>x.y2Strategy.profileHash='0'.repeat(64),x=>x.y2Strategy.sharedSlot=false,x=>x.liveTestApproval.y2Strategy.margin=200,x=>x.strategy.maxPositions=2,x=>x.independentGuards.maximumAggregateNotionalUsd=100000]){const bad=structuredClone(c);mutate(bad);assert.throws(()=>safety.assertLaunchConfig(bad));}
});
test('Y2 alone needs explicit standalone approval and keeps the shared parameter lock',()=>{
 const safety=require('../../apps/vchera2/safety.cjs'),c=structuredClone(safety.config);c.mainStrategy={...c.mainStrategy,enabled:false};c.y2Strategy={enabled:true,id:Y2_ID,profileHash:PROFILE_HASH,sharedSlot:true};c.liveTestApproval.y2Strategy={approved:true,profileHash:PROFILE_HASH,margin:100,leverage:500,standalone:true};assert.ok(safety.assertLaunchConfig(c));
 for(const mutate of[x=>delete x.liveTestApproval.y2Strategy.standalone,x=>x.strategy.stopUsd=99,x=>x.strategy.margin=200,x=>x.y2Strategy.sharedSlot=false,x=>x.independentGuards.maximumAggregateNotionalUsd=100000]){const bad=structuredClone(c);mutate(bad);assert.throws(()=>safety.assertLaunchConfig(bad));}
});
