'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const fs=require('node:fs'),vm=require('node:vm'),path=require('node:path');
const {startBlockers,authorizedControl}=require('../../apps/vchera2/trading-controls.cjs');
const {paperQuote}=require('../../apps/vchera2/paper-quote.cjs');
const {makeHarness}=require('./execution-harness.cjs');
const good={now:1000,margin:2100,available:2300,cutoffReached:false,highWaterTripped:false,pausedUntil:0,keyActive:true,walletFresh:true,marketFresh:true,executionFresh:true,positionCount:0,pendingCount:0,walletPositions:0,walletOperations:0};
test('manual start cannot bypass cutoff/high-water/pause/key/balance/positions or stale reconciliation',()=>{
 assert.deepEqual(startBlockers(good),[]);
 for(const change of [{cutoffReached:true},{highWaterTripped:true},{pausedUntil:2000},{keyActive:false},{available:2172},{walletFresh:false},{marketFresh:false},{executionFresh:false},{positionCount:1},{pendingCount:1},{walletPositions:1},{walletOperations:1}])assert.ok(startBlockers({...good,...change}).length>0,JSON.stringify(change));
});
test('HTTP controls require custom header, loopback host and matching Origin',()=>{
 const req=headers=>({headers});
 assert.equal(authorizedControl(req({host:'127.0.0.1:18762','x-vchera2-control':'1',origin:'http://127.0.0.1:18762'})),true);
 assert.equal(authorizedControl(req({host:'localhost:19000','x-vchera2-control':'1',origin:'http://localhost:19000'})),true);
 assert.equal(authorizedControl(req({host:'127.0.0.1:18762','x-vchera2-control':'1',origin:'https://evil.example'})),false);
 assert.equal(authorizedControl(req({host:'evil.example','x-vchera2-control':'1'})),false);
 assert.equal(authorizedControl(req({host:'127.0.0.1:18762'})),false);
 assert.equal(authorizedControl(req({host:'127.0.0.1:18762','x-vchera2-kill':'1'}),true),true);
});
test('real strategy manual start checks guards and preserves exposure',async()=>{
 const h=makeHarness();await h.api.killSwitch();
 assert.equal(h.test.cfg.live,false);assert.deepEqual(h.manualStops,[true]);
 h.test.executionSync.ready=true;h.test.executionSync.t=Date.now();
 let started=h.api.startTrading();assert.equal(started.ok,true);assert.equal(h.test.cfg.margin,2100);assert.equal(h.test.cfg.leverage,600);assert.deepEqual(h.manualStops,[true,false]);
 h.safety.cutoffReached=()=>true;h.test.cfg.live=false;h.test.cfg.enabled=false;
 started=h.api.startTrading();assert.equal(started.ok,false);assert.match(started.reasons.join(' '),/Окно торговли/);assert.equal(h.test.cfg.live,false);h.db.close();
});
test('manual STOP persists across reload and START clears only manual flag, not high-water',()=>{
 const files=new Map(),config=structuredClone(require('../../apps/vchera2/launch-config.json'));
 delete config.candidate;delete config.mainStrategy;delete config.y2Strategy;config.strategy.margin=2100;config.strategy.leverage=600;
 config.strategy.live=true;config.strategy.enabled=true;
 const date=class extends Date{static now(){return Date.parse('2026-10-20T10:00:00Z');}};
 const source=fs.readFileSync(require.resolve('../../apps/vchera2/safety.cjs'),'utf8');
 function load(){const c={module:{exports:{}},Date:date,require:id=>id==='node:fs'?{readFileSync:name=>{if(!files.has(name)){const e=new Error();e.code='ENOENT';throw e;}return files.get(name);}}:id==='node:path'?path:id==='./launch-config.json'?config:id==='./runtime.cjs'?{privateRuntime:()=>'/test',atomicPrivateJSON:(name,value)=>files.set(name,JSON.stringify(value))}:id==='./session-risk.cjs'?require('../../apps/vchera2/session-risk.cjs'):require(id)};vm.runInNewContext(source,c);return c.module.exports;}
 let safety=load();safety.setManualStop(true);safety=load();
 assert.equal(safety.lockedStrategyConfig({haircut:[]},1).live,false);
 safety.setManualStop(false);assert.equal(safety.lockedStrategyConfig({haircut:[]},1).live,true);
 safety.highWater(1,100);safety.highWater(1,-100);assert.equal(safety.currentHighWater().drawdown,200);
 files.set('/test/high-water.json',JSON.stringify({dayKey:1,tripped:true}));safety=load();assert.equal(safety.lockedStrategyConfig({haircut:[]},1).live,false);
});
test('Paper website quote has exact scale-10 midpoint, weighted price and source time',()=>{
 const q=paperQuote({coin:'BTC',time:123,bbo:[{px:'81583.5',sz:'2'},{px:'81584.0',sz:'1'}]},500);
 assert.equal(q.mid,81583.7);assert.equal(q.bid,81583.5);assert.equal(q.ask,81584);assert.equal(q.sourceTimeMs,123);assert.equal(q.t,500);assert.equal(q.bookNA,true);
 assert.ok(Math.abs(q.weightedPrice-(81583.5+81584*2)/3)<1e-8);assert.match(q.source,/PaperTrade website/);
 assert.throws(()=>paperQuote({coin:'BTC',time:1,bbo:[{px:'1.01',sz:'1'},{px:'2',sz:'1'}]}),/scale/);
 assert.throws(()=>paperQuote({coin:'BTC',time:1,bbo:[{px:'3',sz:'1'},{px:'2',sz:'1'}]}),/Crossed/);
});
