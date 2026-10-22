'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const {entryDeadline,createEntrySender,EntryIntentLedger}=require('../../apps/vchera2/entry-deadline.cjs');
const ID='0x'+'a'.repeat(64),W='0x'+'1'.repeat(40);
const input=()=>({isLong:false,margin:10,leverage:100,instrumentId:0,signalAtMs:10000,validUntilMs:14500});
function fixture(){let clock=10000,saved=null,signs=0;const posts=[],messages=[];
 const ledger=new EntryIntentLedger({wallet:W,read:()=>{if(!saved){const e=new Error();e.code='ENOENT';throw e;}return saved;},save:x=>{saved=structuredClone(x);}});
 const sender=createEntrySender({master:W,takeNonce:()=>String(++signs),now:()=>clock,sign:async m=>{messages.push(m);return{intentId:ID,signed:{...m,testOnly:true}};},post:async b=>{posts.push(b);return{ok:true,status:201,data:{intentId:ID}};},record:x=>ledger.record(x),unresolved:()=>ledger.unresolved()});
 return{sender,ledger,posts,messages,advance:ms=>{clock+=ms;},saved:()=>saved};}
test('entry deadline bound to original signal, seconds equality reserve and no hidden one-hour default',()=>{
 const p=entryDeadline(input(),10000);assert.equal(p.deadline,'13');assert.equal(entryDeadline(p,10800).deadline,'13');
 assert.throws(()=>entryDeadline(p,13000));assert.throws(()=>entryDeadline({...input(),validUntilMs:20000},10000));assert.throws(()=>entryDeadline({...input(),signalAtMs:10001},10000));
});
test('BTC adapter signs exact short deadline and persists only metadata before any POST',async()=>{
 const h=fixture();let prepared;const r=await h.sender(input(),{onPrepared:m=>{prepared=m;assert.equal(h.saved().records[0].state,'unknown');},beforeSend:()=>{assert.equal(h.posts.length,0);}});
 assert.equal(r.ok,true);assert.equal(h.messages[0].deadline,'13');assert.equal(prepared.deadline,'13');assert.equal(h.posts.length,1);assert.equal(h.ledger.unresolved().length,1);
 assert.equal(JSON.stringify(h.saved()).includes('testOnly'),false);
});
test('slow signing never extends expired source signal and never sends',async()=>{
 let now=10000,posts=0;const sender=createEntrySender({master:W,takeNonce:()=>1,now:()=>now,sign:async()=>{now+=2000;return{intentId:ID,signed:{}};},post:async()=>{posts++;},record:()=>{}});
 await assert.rejects(sender(input(),{beforeSend:()=>{}}),/signal expired/);assert.equal(posts,0);
});
test('local expiry/preflight refusal after metadata is proven not_sent, not an invented server cancellation',async()=>{
 const h=fixture();await assert.rejects(h.sender(input(),{beforeSend:()=>{h.advance(10000);}}),/signal expired/);
 assert.equal(h.posts.length,0);assert.equal(h.saved().records[0].state,'not_sent');assert.equal(h.saved().records[0].proof,'LOCAL_ABORT_BEFORE_FIRST_TRANSPORT_CALL');assert.equal(h.ledger.unresolved().length,0);
});
test('uncertain actual POST remains unknown across reload/expiry; only exact owned intent position resolves it',async()=>{
 const h=fixture();await h.sender(input(),{beforeSend:()=>{}});let saved=h.saved();
 const reloaded=new EntryIntentLedger({wallet:W,read:()=>saved,save:x=>{saved=x;}});assert.equal(reloaded.unresolved().length,1);
 reloaded.observePositions([{intentId:'0x'+'b'.repeat(64),positionId:'8'}]);assert.equal(reloaded.unresolved().length,1);
 reloaded.observePositions([{intentId:ID,positionId:'7'}]);assert.equal(reloaded.unresolved().length,0);assert.equal(saved.records[0].state,'filled');
 await assert.rejects(h.sender(input(),{beforeSend:()=>{}}),/Unresolved/);assert.equal(h.posts.length,1);
});
test('ETH remains observe-only and durable write failure prevents transport',async()=>{
 const h=fixture();await assert.rejects(h.sender({...input(),instrumentId:1},{beforeSend:()=>{}}),/observe-only/);assert.equal(h.messages.length,0);
 let posts=0;const sender=createEntrySender({master:W,takeNonce:()=>1,now:()=>10000,sign:async()=>({intentId:ID,signed:{}}),post:async()=>{posts++;},record:()=>{throw new Error('disk full');}});
 await assert.rejects(sender(input(),{beforeSend:()=>{}}),/disk full/);assert.equal(posts,0);
});
test('corrupt entry ledger blocks new submissions rather than discarding unknown exposure',async()=>{
 const ledger=new EntryIntentLedger({wallet:W,read:()=>({schemaVersion:1,wallet:W,records:[{}]}),save:()=>{}});assert.ok(ledger.error);assert.equal(ledger.unresolved().length,1);assert.throws(()=>ledger.record({intentId:ID,state:'unknown'}));
});
test('durable metadata callback is awaited before actual transport',async()=>{
 let release,post=0;const barrier=new Promise(r=>{release=r;});const sender=createEntrySender({master:W,takeNonce:()=>1,now:()=>10000,sign:async()=>({intentId:ID,signed:{}}),post:async()=>{post++;return{ok:true,data:{intentId:ID}};},record:async()=>barrier});
 const pending=sender(input(),{beforeSend:()=>{}});await new Promise(r=>setImmediate(r));assert.equal(post,0);release();await pending;assert.equal(post,1);
});
test('actual BTC strategy forwards original signal deadline and retains it through async preparation',async()=>{
 const {makeHarness}=require('./execution-harness.cjs'),h=makeHarness();const signalAtMs=Date.now()-200,validUntilMs=signalAtMs+5000;let captured,options;
 h.account.open=async(input,hooks)=>{captured=input;options=hooks;return{ok:false,uncertain:true,intentId:'0x'+'a'.repeat(64)};};
 h.test.liveEntry('book','S',{sig:.7,books:[],dev:1,signalAtMs,validUntilMs});await new Promise(r=>setImmediate(r));
 assert.equal(captured.instrumentId,0);assert.equal(captured.signalAtMs,signalAtMs);assert.equal(captured.validUntilMs,validUntilMs);assert.equal(h.test.pending[0].createdAt,signalAtMs);
 h.test.cfg.live=false;assert.throws(()=>options.beforeSend(),/LIVE\/signals/);h.db.close();
});

test('configured signed validity: default 5 s unchanged, explicit 3600 s accepted, out of bounds refused',()=>{
 const {entryDeadline,configuredValidityMs}=require('../../apps/vchera2/entry-deadline.cjs');const now=Date.now(),signalAtMs=now-200;
 assert.throws(()=>entryDeadline({signalAtMs,validUntilMs:signalAtMs+60000},now),/5000 ms policy/);
 const p=entryDeadline({signalAtMs,validUntilMs:signalAtMs+3600000,maxValidityMs:3600000},now);assert.equal(p.maxValidityMs,3600000);assert.equal(Number(p.deadline),Math.floor((signalAtMs+3600000)/1000)-1);
 assert.throws(()=>entryDeadline({signalAtMs,validUntilMs:signalAtMs+6000,maxValidityMs:4000},now),/5000\.\.3600000/);
 assert.equal(configuredValidityMs({independentGuards:{}}),5000);assert.equal(configuredValidityMs({independentGuards:{entrySignedValidityMs:3600000}}),3600000);
 assert.throws(()=>configuredValidityMs({independentGuards:{entrySignedValidityMs:7200000}}),/5000\.\.3600000/);
});

test('unknown entry resolves as expired only after signed deadline + grace on a fresh wallet without the intent',()=>{
 const {EntryIntentLedger}=require('../../apps/vchera2/entry-deadline.cjs');let saved=null;
 const l=new EntryIntentLedger({wallet:W,read:()=>{const e=new Error('none');e.code='ENOENT';throw e;},save:d=>{saved=d;}});
 l.record({intentId:ID,action:'openPosition',instrumentId:0,state:'unknown',signalAtMs:1000,validUntilMs:21000,deadline:'19',preparedAtMs:1001});
 l.resolveExpired({walletT:40000,positions:[],operationIds:[]});assert.equal(l.unresolved().length,1,'too early: 40 s < (19+1) s + 30 s');
 l.resolveExpired({walletT:60000,positions:[],operationIds:[ID]});assert.equal(l.unresolved().length,1,'operation still present');
 l.resolveExpired({walletT:60000,positions:[{intentId:ID,positionId:'1'}],operationIds:[]});assert.equal(l.unresolved().length,1,'position present');
 l.resolveExpired({walletT:60000,positions:[],operationIds:[]});assert.equal(l.unresolved().length,0);assert.equal(saved.records[0].state,'expired');
 assert.throws(()=>l.record({intentId:ID,state:'expired'}),/expiry evidence/);
});

test('explicit admission/deadline rejection is durable and terminal; conflicts, transport errors and mismatched IDs remain unknown',async()=>{
 for(const [response,rejected] of [
  [{ok:false,status:409,data:{code:'admission_rejected'},uncertain:false},true],
  [{ok:false,status:422,data:{code:'deadline_expired'},uncertain:false},true],
  [{ok:false,status:409,data:{code:'intent_conflict'},uncertain:false},false],
  [{ok:false,status:500,data:{code:'admission_rejected'},uncertain:true},false],
  [{ok:false,status:0,data:{code:'transport_unknown'},uncertain:true},false],
  [{ok:false,status:409,data:{code:'admission_rejected',intentId:'0x'+'b'.repeat(64)},uncertain:false},false]
 ]){
  let saved=null,posts=0;const ledger=new EntryIntentLedger({wallet:W,read:()=>{const e=new Error();e.code='ENOENT';throw e;},save:r=>{saved=structuredClone(r);}});
  const sender=createEntrySender({master:W,takeNonce:()=>1,now:()=>10000,sign:async()=>({intentId:ID,signed:{}}),post:async()=>{posts++;return response;},record:r=>ledger.record(r),unresolved:()=>ledger.unresolved()});
  const r=await sender(input(),{beforeSend:()=>{}});assert.equal(posts,1);assert.equal(r.rejected,rejected);assert.equal(r.uncertain,!rejected);
  const reload=new EntryIntentLedger({wallet:W,read:()=>saved,save:()=>{}});assert.equal(reload.unresolved().length,rejected?0:1);
  assert.equal(saved.records[0].state,rejected?'rejected':'unknown');
 }
});

test('new entry is blocked before signing while an exit or another operation owns the slot',()=>{
 const {makeHarness}=require('./execution-harness.cjs'),h=makeHarness();let attempts=0;h.account.open=async()=>{attempts++;return{ok:true};};
 const ctx={sig:7,books:[],dev:7,signalAtMs:Date.now(),validUntilMs:Date.now()+5000};
 h.test.positions.set(1,{id:1,live:true,positionId:'42',closing:{intentId:'exit-pending'}});
 h.test.liveEntry('spread','L',ctx);assert.equal(attempts,0);assert.equal(h.test.pending.length,0);
 h.test.positions.clear();h.account.wallet.operations=[['exit-pending',{}]];
 h.test.liveEntry('spread','L',ctx);assert.equal(attempts,0);assert.equal(h.test.pending.length,0);h.db.close();
});

test('known rejected entry releases local slot without inventing a cancel or unknown outcome',async()=>{
 const {makeHarness}=require('./execution-harness.cjs'),h=makeHarness();h.account.open=async()=>({ok:false,status:409,data:{code:'admission_rejected'},rejected:true,uncertain:false,intentId:ID});
 h.test.liveEntry('spread','S',{sig:7,books:[],dev:7,signalAtMs:Date.now(),validUntilMs:Date.now()+5000});
 await new Promise(r=>setImmediate(r));assert.equal(h.test.pending.length,0);assert.equal(h.test.cancelledLive.size,0);h.db.close();
});
