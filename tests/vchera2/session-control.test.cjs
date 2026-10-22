'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const {makeHarness}=require('./execution-harness.cjs');
const iso=n=>new Date(n).toISOString();
const session=(start,end)=>({id:'integration',startAt:iso(start),endAt:iso(end),lossLimitUsd:150});
function ready(h){h.test.executionSync.ready=true;h.test.executionSync.t=Date.now();}
test('real executor refuses early manual START and direct entry without signing',()=>{
 const now=Date.now(),h=makeHarness(null,{session:session(now+60000,now+3600000)});
 try{ready(h);h.test.cfg.live=false;h.test.cfg.enabled=false;
  const r=h.api.startTrading();assert.equal(r.ok,false);assert.ok(r.reasons.includes('LIVE_SESSION_NOT_STARTED'));
  assert.equal(h.test.liveEntry('spread','L',{signalAtMs:now,validUntilMs:now+20000,books:[]}),null);assert.equal(h.calls.length,0);
 }finally{h.db.close();}
});
test('real guard stops LIVE at fee-inclusive session loss and refuses rearm',async()=>{
 const now=Date.now(),h=makeHarness(null,{session:session(now-60000,now+3600000)});
 try{ready(h);
  h.db.prepare("INSERT INTO btcy2_lag_trades(opened_at,closed_at,side,entry,qty,margin,leverage,status,live,position_id,real_pnl,close_evidence) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)").run(now-30000,now-1000,'L',80000,1,100,500,'closed',1,'test-session',-149,JSON.stringify({fee:1}));
  h.test.guardTick();await new Promise(r=>setImmediate(r));assert.equal(h.test.cfg.live,false);assert.equal(h.test.cfg.enabled,false);assert.deepEqual(h.manualStops,[true]);
  const r=h.api.startTrading();assert.equal(r.ok,false);assert.ok(r.reasons.includes('LIVE_SESSION_LOSS_LIMIT'));
 }finally{h.db.close();}
});
test('real guard at session end persists manual STOP',async()=>{
 const now=Date.now(),h=makeHarness(null,{session:session(now-3600000,now-1)});
 try{h.test.guardTick();await new Promise(r=>setImmediate(r));assert.equal(h.test.cfg.live,false);assert.deepEqual(h.manualStops,[true]);assert.equal(h.api.getState({light:true}).liveSession.reason,'LIVE_SESSION_ENDED');}
 finally{h.db.close();}
});
test('unresolved close PnL blocks entries at once and does not stop LIVE immediately',async()=>{
 const now=Date.now(),h=makeHarness(null,{session:session(now-60000,now+3600000)});
 try{ready(h);
  h.db.prepare("INSERT INTO btcy2_lag_trades(opened_at,closed_at,side,entry,qty,margin,leverage,status,live,position_id,real_pnl,close_evidence) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)").run(now-30000,now-1000,'L',80000,1,100,500,'closed',1,'test-unresolved',null,null);
  h.test.guardTick();await new Promise(r=>setImmediate(r));assert.equal(h.test.cfg.live,true);assert.deepEqual(h.manualStops,[]);
  assert.equal(h.test.liveEntry('spread','L',{signalAtMs:now,validUntilMs:now+20000,books:[]}),null);assert.equal(h.calls.length,0);
  assert.equal(h.api.getState({light:true}).liveSession.entryBlockReason,'LIVE_SESSION_PNL_UNRESOLVED');
 }finally{h.db.close();}
});
