'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const {TimingGate}=require('../src/latency.js');
function sample(){return{snapshot:{ok:true,stale:false,ts:10000,totalPendingGas:1000,totalQueuedGas:1000,totalSubmittedGas:0,pendingCount:1,chain:{blocks:[1,2,3,4,5].map(number=>({number,timestampMs:5000+number*1000,papertradeGasUsed:number===3?999999999:100000,kind:number===3?'big':'ordinary'}))}}};}
test('known big block proves coverage without being counted as ordinary drain',()=>{const g=new TimingGate();g.observe(sample(),10000);const r=g.evaluate(10000);assert.equal(r.allow,true);assert.equal(r.sample_blocks,4);assert.equal(r.drain_gas_per_second,80000);});
test('an actually missing block still blocks timing admission',()=>{const p=sample();p.snapshot.chain.blocks=p.snapshot.chain.blocks.filter(b=>b.number!==3);const g=new TimingGate();g.observe(p,10000);assert.deepEqual(g.evaluate(10000).reasons,['BLOCK_COVERAGE_GAP']);});
test('changing known block kind cannot silently create coverage or service',()=>{const g=new TimingGate(),p=sample();g.observe(p,10000);p.snapshot.chain.blocks[2].kind='ordinary';g.observe(p,10001);assert.ok(g.evaluate(10001).reasons.includes('BLOCK_REVISION_CONFLICT'));});
