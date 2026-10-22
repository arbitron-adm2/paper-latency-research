'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const {floorUsdc,liveSettingsContext}=require('../../apps/vchera2/live-settings-context.cjs');
test('funds context floors fractional micro USDC and never edits the wallet object',()=>{
 const s={LIVE:false,entriesEnabled:false,providerFresh:true,openManaged:[],provider:{wallet:{available:1980.6933479,balance:2000.1234569,t:Date.now(),connected:true,openPositions:[],operations:[]},entrySafety:{unknown:[]}}};
 const c=liveSettingsContext(s);assert.equal(c.provider.wallet.available,'1980.693347');assert.equal(c.provider.wallet.balance,'2000.123456');assert.equal(s.provider.wallet.available,1980.6933479);assert.notEqual(c.provider.wallet,s.provider.wallet);
});
test('unknown funds and authority remain unknown, never convert a draft into cash approval',()=>{
 for(const value of[NaN,Infinity,-1,'100',null,1e10])assert.equal(floorUsdc(value),null);
 const c=liveSettingsContext({LIVE:false,entriesEnabled:false,providerFresh:false,provider:{wallet:{available:NaN,balance:NaN}}});assert.equal(c.provider.wallet.available,null);assert.equal(c.riskBudget.confirmed,false);assert.equal(c.instrumentLimits.confirmed,false);assert.equal(c.safetyReady,false);assert.equal(c.contractCapability.btcEntryWithDeadline,false);
});
