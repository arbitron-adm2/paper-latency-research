'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const {createSummaryExecutionClass,installSummaryExecutionPrice}=require('../../apps/strategy-lab/summary-execution.cjs');
test('external price points are tagged and past-only; genuine BBO takes priority',()=>{
 let own=null;const e={config:{executionPricePolicy:{}},executionQuote:()=>own},b={summary:{at:1000,mid:100,sourceId:4,sourceRecordId:'42'}};installSummaryExecutionPrice(e,b);
 assert.equal(e.executionQuote(999),null);const q=e.executionQuote(1001);assert.equal(q.mid,100);assert.equal(q.bid,undefined);assert.equal(q.ask,undefined);assert.equal(q.priceKind,'external-summary-mid');assert.equal(e.executionQuote(2501),null);
 own={bid:99,ask:101};assert.equal(e.executionQuote(1001),own);
});
test('research execution class reads a typed scalar without modifying the production engine',()=>{
 const {Execution:Original}=require('../../apps/papertrade-shadow/engine.cjs'),{buildProfile}=require('../../apps/papertrade-shadow/persistent-profile.cjs'),C=require('../../apps/papertrade-shadow/state-codec.cjs');
 const Scalar=createSummaryExecutionClass(),p=C.clone(buildProfile()),T=1792496110000,e=new Scalar(p.config,p.experiment,p.scenario,T);e.bridge={api:{feed:{},positions:new Map()}};
 installSummaryExecutionPrice(e,{summary:{at:T,mid:81437.5}});assert.equal(e.quote(),814375n);assert.notEqual(Scalar,Original);assert.equal(require('../../apps/papertrade-shadow/engine.cjs').Execution,Original);
});
test('physical scenario fill retains scalar provenance without fake quote fields',()=>{
 const {buildProfile}=require('../../apps/papertrade-shadow/persistent-profile.cjs'),C=require('../../apps/papertrade-shadow/state-codec.cjs');const p=C.clone(buildProfile()),T=1792496110000;
 p.experiment.margin=100;p.experiment.leverage=500;p.config.limits.minimumNotional='1000';const e=new (createSummaryExecutionClass())(p.config,p.experiment,p.scenario,T),fills=[];
 e.bridge={setTime:()=>{},api:{feed:{},positions:new Map(),fill:(q,x)=>fills.push(x),terminal:()=>{}}};e.entryPolicyReason=()=>null;e.delay=()=>1000;e.inputHealthy=true;
 installSummaryExecutionPrice(e,{summary:{at:T,mid:81437.5,sourceId:4109,sourceRecordId:'42'}});const id=e.submit({margin:100,side:'L',strategy:'spread',ctx:{}});const timer=e.timers.find(x=>x.id===id&&x.kind==='entry');assert.ok(timer);e.now=timer.at;e.fire(timer);
 assert.equal(e.positions.size,1);assert.equal(fills.length,1);const q=[...e.positions.values()][0].entryPriceEvidence;assert.equal(q.mid,81437.5);assert.equal(q.priceKind,'external-summary-mid');assert.equal(q.bid,undefined);assert.equal(q.sourceRecordId,'42');
});
