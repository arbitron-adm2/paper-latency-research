'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const {DatabaseSync}=require('node:sqlite');
const {MainBtcRuntime,PROFILE_HASH,ID}=require('../../apps/vchera2/main-btc-runtime.cjs');
function fixture(){
 let now=Date.parse('2026-10-21T01:30:00Z');const db=new DatabaseSync(':memory:');
 db.exec('CREATE TABLE btcy2_lag_trades(strategy TEXT,live INTEGER,status TEXT,qty REAL,real_pnl REAL)');
 const marketData={onEvent:fn=>{marketData.listener=fn;return()=>{};}};
 const opts={db,marketData,now:()=>now};const r=new MainBtcRuntime(opts);
 function feed(px=100000){for(const venue of ['hl','bf','bs']){const price=venue==='hl'?px:100000;for(const kind of ['quote','book'])r.ingest({kind:'market',symbol:'BTC',venue,recv_ms:now,source_ms:now,payload:{frameKind:kind,bid:String(price-.5),ask:String(price+.5),bids:[[String(price-.5),'1']],asks:[[String(price+.5),'1']]}});}}
 function order(side='L',intent='0x'+'a'.repeat(64)){return{intentId:intent,createdAt:now,validUntilMs:now+20000,margin:100,leverage:500,side,ctx:{hl:100000,dev:8,spread:8,baseline:0,binMove:8,books:[],nativeRequestId:'local-test',nativeStrategy:'spread',sig:8}};}
 const provider={wallet:{available:2300,t:now,operations:[],openPositions:[]},entrySafety:{unknown:[]}};
 return{r,db,opts,provider,feed,order,now:()=>now,advance:ms=>{now+=ms;provider.wallet.t=now;}};
}
for(const side of ['L','S'])test('actual '+side+' fill is adopted once, source stop requests close without inventing a fill',()=>{
 const f=fixture(),q=f.order(side),p={id:1,strategy:ID,side,live:true,positionId:'42',intentId:q.intentId,entry:100000,margin:100,leverage:500,qty:.5,opened_at:f.now(),maxFav:0,maxAdv:0};
 f.feed();f.r.remember(q);const context={positions:new Map([[1,p]]),pending:[],provider:f.provider};
 f.r.sync(context);f.r.sync(context);assert.equal(f.r.bridge.api.positions.size,1);
 assert.equal(f.db.prepare('SELECT count(*) n FROM sim_native_main_btc_live_trades').get().n,1);
 f.advance(100);f.feed(side==='L'?99995:100005);f.r.advance(f.now(),false);
 assert.equal(f.r.closes.length,1);assert.equal(f.r.closes[0].positionId,'42');assert.match(f.r.closes[0].why,/стоп/);
 assert.equal(f.r.bridge.api.positions.size,1,'close request is not a fill');
 f.r.closed(p,{px:side==='L'?99994:100006,ts:f.now()+5000,realPnl:-3});
 assert.equal(f.r.bridge.api.positions.size,0);
 assert.equal(f.db.prepare('SELECT real_pnl FROM sim_native_main_btc_live_trades').get().real_pnl,-3);
 assert.equal(f.db.prepare('SELECT state FROM main_btc_live_intents').get().state,'closed');f.db.close();
});
test('unknown short intent recovers the original identity and side without a second submission',()=>{
 const f=fixture(),q=f.order('S');f.r.remember(q);f.provider.entrySafety.unknown=[{intentId:q.intentId}];
 const state={pending:[],positions:new Map(),cancelledLive:new Map(),provider:f.provider};f.r.recover(state);f.r.recover(state);
 assert.equal(state.pending.length,1);assert.equal(state.pending[0].intentId,q.intentId);assert.equal(state.pending[0].side,'S');assert.equal(f.r.requests.length,0);f.db.close();
});
test('restart restores source position state without duplicating the native trade',()=>{
 const f=fixture(),q=f.order(),p={id:1,strategy:ID,side:'L',live:true,positionId:'42',intentId:q.intentId,entry:100000,margin:100,leverage:500,qty:.5,opened_at:f.now(),maxFav:0,maxAdv:0};
 f.feed();f.r.remember(q);const state={positions:new Map([[1,p]]),pending:[],provider:f.provider};f.r.sync(state);
 const source=[...f.r.bridge.api.positions.values()][0];source.maxFav=12;source.bBest=100030;f.r.save();
 const r2=new MainBtcRuntime(f.opts);r2.sync(state);assert.equal([...r2.bridge.api.positions.values()][0].maxFav,12);assert.equal([...r2.bridge.api.positions.values()][0].bBest,100030);
 assert.equal(f.db.prepare('SELECT count(*) n FROM sim_native_main_btc_live_trades').get().n,1);f.db.close();
});
test('warmup and missing realised cash block entries while shared feed keeps updating',()=>{
 const f=fixture();f.feed();assert.match(f.r.blockers().join(';'),/WARMUP/);f.advance(180001);f.feed();assert.deepEqual(f.r.blockers(),[]);
 f.db.prepare('INSERT INTO btcy2_lag_trades VALUES(?,1,\'closed\',.5,NULL)').run(ID);assert.match(f.r.blockers().join(';'),/PNL_NOT_RECONCILED/);f.db.close();
});
test('approval binds the exact profile, size, entry rules and financial ceiling',()=>{
 const safety=require('../../apps/vchera2/safety.cjs'),c=structuredClone(safety.config);
 assert.equal(c.mainStrategy.profileHash,PROFILE_HASH);assert.equal(safety.assertLaunchConfig(c),true);
 for(const mutate of [x=>x.strategy.margin=200,x=>x.strategy.roundZoneUsd=50,x=>x.strategy.minDev=3,x=>x.independentGuards.maximumAggregateNotionalUsd=100000,x=>x.independentGuards.entrySignedValidityMs=3600000]){const b=structuredClone(c);mutate(b);assert.throws(()=>safety.assertLaunchConfig(b));}
});

test('selected main profile retains a late fill for native exits while manual STOP still closes it',async()=>{
 const {makeHarness}=require('./execution-harness.cjs');
 for(const stopped of [false,true]){
  const h=makeHarness(null,{main:true});if(stopped)h.test.cfg.live=false;
  const p=h.test.openPosition(ID,'L',{books:[],dev:8,fill:{positionId:'42',intentId:'0x'+'a'.repeat(64),entry:100000,margin:100,leverage:500,openedAt:Date.now(),invalidatedReason:'сигнал пропал после отправки'}});
  assert.equal(!!p.closing,stopped);h.test.mainStrategy.detach();await new Promise(r=>setImmediate(r));h.db.close();
 }
});
