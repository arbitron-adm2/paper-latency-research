'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),crypto=require('node:crypto');
const {SummaryBooks}=require('../../apps/strategy-lab/summary-books.cjs');
const q=(at=1000)=>({kind:'market',symbol:'BTC',venue:'bf',source_ms:at,recv_ms:at,payload:{frameKind:'quote',bid:'100',ask:'101',bidSize:'3',askSize:'1'}});
const s=(at=1000)=>({kind:'book_summary',symbol:'BTC',venue:'hl',recv_ms:at,payload:{mid:100,imbalancePct:69,bidNearUsd:300,askNearUsd:2,wallUsd:null}});
test('summaries never become price quotes or artificial book levels; future/stale snapshots are refused',()=>{const x=new SummaryBooks();x.observe(s());assert.equal(x.at('hl',999),null);assert.deepEqual({...x.at('hl',1000)},{imb:.69,l1b:300,l1a:2,researchKind:'Vlad exported summary; band/rounding/source age not independently verified',wall:null});assert.equal(x.at('hl',2501),null);assert.equal(x.summary.bid,undefined);assert.equal(x.summary.bids,undefined);});
test('strict policy cannot recover missing BF depth from BBO quantities',()=>{const x=new SummaryBooks();x.observe(q());assert.equal(x.at('bf',1000),null);});
test('L1 proxy is explicit, size aware and past-only; fresh raw depth takes priority',()=>{const x=new SummaryBooks({policy:'bf_l1_proxy'});x.observe(q());assert.equal(x.at('bf',999),null);const r=x.at('bf',1000);assert.equal(r.l1b,300);assert.equal(r.l1a,101);assert.equal(r.imb,199/401);x.feed={bf:{bids:[[100,1]],asks:[[101,1]],bookT:1000}};assert.equal(x.at('bf',1000),null);});
test('empty latest summary removes stale good features instead of backfilling',()=>{const x=new SummaryBooks();x.observe(s());x.observe({...s(1100),payload:{imbalancePct:null}});assert.equal(x.at('hl',1100),null);});
test('research factory leaves immutable source bytes and default bridge manifest unchanged',()=>{const original=require('../../apps/papertrade-shadow/strategy.cjs'),before=original.sourceManifest();const {createSummaryBridgeFactory}=require('../../apps/strategy-lab/summary-bridge.cjs');const factory=createSummaryBridgeFactory();assert.deepEqual(original.sourceManifest(),before);const altered=factory.sourceManifest();assert.notEqual(altered[0].sha256,before[0].sha256);assert.deepEqual(altered.slice(1),before.slice(1));});
test('native VM consumes explicit summaries without fake levels or summary execution quotes',()=>{
 const {Execution}=require('../../apps/papertrade-shadow/engine.cjs'),{buildProfile}=require('../../apps/papertrade-shadow/persistent-profile.cjs'),C=require('../../apps/papertrade-shadow/state-codec.cjs'),{createSummaryBridgeFactory}=require('../../apps/strategy-lab/summary-bridge.cjs');
 const p=C.clone(buildProfile()),T=1792480000000,e=new Execution(p.config,p.experiment,p.scenario,T);e.researchBooks=new SummaryBooks({policy:'bf_l1_proxy'});
 const b=createSummaryBridgeFactory().createBridge(e,p.launch,p.experiment,T);e.bridge=b;e.researchBooks.feed=b.api.feed;e.inputHealthy=true;e.integrityWarmUntil=T;
 try{
  const quote=(v,bid,ask)=>({kind:'market',symbol:'BTC',market:'BTC',venue:v,recv_ms:T,event_ms:T,source_ms:T,payload:{frameKind:'quote',bid:String(bid),ask:String(ask),bidSize:'3',askSize:'1',sourceTimestampKnown:true}});
  for(const q of [quote('hl',100,100.1),quote('bf',101,101.1)]){e.researchBooks.observe(q);b.market(q);}
  e.researchBooks.observe({...s(T),payload:{...s(T).payload,mid:999999}});
  b.step(T+100);assert.equal(e.lastInputChecks.actualFullDepth,false);assert.equal(e.lastInputChecks.fullDepth,true);assert.deepEqual(Array.from(b.api.feed.hl.bids),[]);assert.equal(b.api.feed.hl.bid,100);
  assert.equal(e.executionPrices.at(T+100).bid,100);assert.notEqual(e.executionPrices.at(T+100).bid,999999);
 }finally{b.close();}
});
