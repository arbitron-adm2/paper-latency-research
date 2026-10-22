'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const {installObservation,installTrainQueue,sizingFor,installExpiryReconciliation}=require('../../apps/live-parity-backtest/strategy-replay.cjs');

function fixture(){
  const calls=[],timers=[];
  const api={fill:(q,x)=>calls.push(['entry',x.positionId]),closePosition:(...args)=>calls.push(['exit',args[0].positionId]),positions:new Map(),closePx:new Map()};
  const e={now:1000,rng:{next:()=>0},bridge:{api},positions:new Map([['p1',{}]]),counts:{lateFill:0},orders:[],
    schedule:(at,kind,id)=>timers.push({at,kind,id}),note:()=>{},fire:()=>calls.push(['original'])};
  installObservation(e,{entryObservationMs:[900],exitObservationMs:[1300]});
  return{e,api,calls,timers};
}
test('physical entry exists before source can act on observed fill',()=>{
  const {e,api,calls,timers}=fixture();
  api.fill({intentId:'i1'},{positionId:'p1'});
  assert.equal(e.positions.size,1);assert.equal(calls.length,0);assert.equal(timers[0].at,1900);
  e.now=1900;e.fire(timers[0]);assert.deepEqual(calls,[['entry','p1']]);
  e.fire(timers[0]);assert.equal(calls.length,1);
});
test('close visibility waits while physical bookkeeping is untouched',()=>{
  const {e,api,calls,timers}=fixture();api.closePosition({positionId:'p1'},'exit',100,1000);
  assert.equal(calls.length,0);assert.equal(timers[0].at,2300);
  e.now=2300;e.fire(timers[0]);assert.deepEqual(calls,[['exit','p1']]);
});
test('signal lost after physical execution is late when fill becomes visible',()=>{
  const {e,api,timers}=fixture(),q={intentId:'i1'};
  api.fill(q,{positionId:'p1'});q.signalGone=true;
  e.now=1900;e.fire(timers[0]);assert.equal(e.positions.get('p1').late,true);assert.equal(e.counts.lateFill,1);
});
test('block delay depends on current second and never future label',()=>{
  const e={now:1250,rng:{next:()=>0},experiment:{margin:20,leverage:100},delay:()=>500,scenario:Object.freeze({}),config:{entryDeadline:{}}};
  installTrainQueue(e,{entryBlocks:[4],exitBlocks:[8],cancelSuccessProbability:.1});
  assert.equal(e.delay('entry',2000),3750);assert.equal(e.exitDelay(),7750);
  assert.equal(e.delay('cancel',2000),500);
});
test('nominal 1.2 million is 2000 margin x600, separately from 2100 x600',()=>{
  const plan={variantSettings:{large:{sizing:{margin:2000,leverage:600}}}};
  assert.equal(sizingFor(plan,'large').notional,1200000);
  assert.equal(sizingFor({sizing:{margin:2100,leverage:600}},'x').notional,1260000);
  assert.throws(()=>sizingFor({sizing:{margin:2400,leverage:600}},'x'),/INVALID_RESEARCH_SIZING/);
});
test('signed expiry releases an unfilled intent only after its deadline and grace',()=>{
  const timers=[],r={id:'i',state:'queued',entryLimit:{deadlineSec:19}};
  const e={now:0,intents:new Map([['i',r]]),submit:()=>r.id,fire:()=>{},note:()=>{},
    schedule:(at,kind,id)=>timers.push({at,kind,id}),expireEntry:x=>{x.state='expired';}};
  installExpiryReconciliation(e);e.submit({});
  assert.equal(r.state,'queued');assert.equal(timers[0].at,50000);
  e.now=50000;e.fire(timers[0]);assert.equal(r.state,'expired');
});
test('offline runner cannot load an HTTP client',()=>{
  assert.throws(()=>require('node:http'),/OFFLINE_NETWORK_DENIED/);
});
