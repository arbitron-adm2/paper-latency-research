'use strict';
// Disabled by default. Dependency-injected transport; importing never reads keys or sends requests.
const crypto=require('node:crypto'),C=require('../papertrade-shadow/state-codec.cjs');
function profileHash(profile){return C.hash(C.stringify(profile));}
function preflight(plan,context,approval=null,now=Date.now()){
 const reasons=[],wallet=context?.provider?.wallet,capital=plan?.capital,hash=profileHash(plan),asset=plan?.profile?.asset;
 if(!['BTC','ETH'].includes(asset))reasons.push('PROFILE_ASSET_REQUIRED');
 if(!capital||!Number.isFinite(capital.budgetUsdc)||!Number.isFinite(capital.marginUsdc)||!Number.isInteger(capital.leverage)||capital.budgetUsdc<=0||capital.marginUsdc<=0||capital.marginUsdc>capital.budgetUsdc||capital.leverage<1||capital.leverage>1000)reasons.push('CAPITAL_NOT_SELECTED');
 if(!approval||approval.hash!==hash||!Number.isSafeInteger(approval.expiresMs)||approval.expiresMs<=now||approval.wallet?.toLowerCase()!==plan?.wallet?.toLowerCase())reasons.push('EXACT_PROFILE_NOT_APPROVED');
 if(!wallet||wallet.connected!==true||!context.providerFresh||!Number.isFinite(wallet.t)||now-wallet.t<0||now-wallet.t>11000)reasons.push('WALLET_NOT_FRESH');
 const available=Number(wallet?.available);if(!Number.isFinite(available)||capital&&(capital.budgetUsdc>available||capital.marginUsdc*1.05>Math.min(capital.budgetUsdc,available)))reasons.push('CAPITAL_OR_RESERVE_EXCEEDS_AVAILABLE');
 if(!Array.isArray(wallet?.openPositions)||wallet.openPositions.length||!Array.isArray(wallet?.operations)||wallet.operations.length)reasons.push('WALLET_NOT_FLAT');
 if(!Array.isArray(context?.provider?.entrySafety?.unknown)||context.provider.entrySafety.unknown.length)reasons.push('UNKNOWN_ENTRY_OUTCOME');
 if(context?.LIVE||context?.entriesEnabled||context?.exchangeMutationsEnabled)reasons.push('OTHER_LIVE_OWNER_ACTIVE');
 if(context?.marketFresh!==true)reasons.push('MARKET_NOT_FRESH');
 if(context?.authorizationReady!==true)reasons.push('SESSION_AUTHORIZATION_NOT_READY');
 if(context?.transportCapabilities?.[asset]?.signedDeadline!==true||context?.transportCapabilities?.[asset]?.independentClose!==true)reasons.push('INSTRUMENT_TRANSPORT_NOT_VERIFIED');
 if(context?.singleStrategyOwner!==true)reasons.push('SINGLE_LIVE_STRATEGY_OWNER_NOT_BOUND');
 return{ready:!reasons.length,reasons,hash,asset,notionalUsd:capital?capital.marginUsdc*capital.leverage:null,availableUsdc:Number.isFinite(available)?available:null,mode:'PREFLIGHT_NO_ORDER',realSignatures:0};
}
class LiveGateway{
 commit(row){const result=this.journal.append(row);if(result&&typeof result.then==='function')throw new Error('SYNCHRONOUS_DURABLE_JOURNAL_REQUIRED');return result;}
 constructor({plan,approval=null,transport,readContext,journal,now=Date.now}){Object.assign(this,{plan,approval,transport,readContext,journal,now});this.hash=profileHash(plan);this.pending=null;this.positions=new Map();if(!journal||typeof journal.append!=='function')throw new Error('DURABLE_JOURNAL_REQUIRED');}
 async submit(signal){
  if(this.pending)throw new Error('ENTRY_PENDING_RECONCILE_FIRST');
  const at=this.now();if(signal.profileHash!==this.hash||!Number.isSafeInteger(signal.atMs)||signal.atMs>at||at-signal.atMs>1000||!['L','S'].includes(signal.side))throw new Error('SIGNAL_PROFILE_OR_TIME_INVALID');
  const check=preflight(this.plan,await this.readContext(),this.approval,at);if(!check.ready)throw new Error(check.reasons.join('|'));
  if(!this.transport?.open)throw new Error('LIVE_TRANSPORT_NOT_BOUND');
  const attempt={id:crypto.randomUUID(),profileHash:this.hash,asset:this.plan.profile.asset,state:'PREPARING',createdMs:at};this.commit(attempt);this.pending=attempt;
  try{
   const result=await this.transport.open({instrumentId:attempt.asset==='BTC'?0:1,isLong:signal.side==='L',margin:this.plan.capital.marginUsdc,leverage:this.plan.capital.leverage,signalAtMs:signal.atMs,validUntilMs:signal.atMs+5000},{onPrepared:meta=>{this.commit({...attempt,state:'PREPARED',intentId:meta.intentId});attempt.intentId=meta.intentId;},beforeSend:async()=>{const c=await this.readContext();const unknown=c.provider?.entrySafety?.unknown;const scoped=Array.isArray(unknown)?{...c,provider:{...c.provider,entrySafety:{...c.provider.entrySafety,unknown:unknown.filter(x=>x.intentId!==attempt.intentId)}}}:c;const latest=preflight(this.plan,scoped,this.approval,this.now());if(!latest.ready)throw new Error(latest.reasons.join('|'));}});
   attempt.intentId=result.intentId??attempt.intentId;attempt.state='AWAITING_PROVIDER_OUTCOME';this.commit({...attempt,acknowledged:!!result.ok,uncertain:!!result.uncertain});return{...result,attemptId:attempt.id,state:attempt.state};
  }catch(error){attempt.state=error.knownNotSent?'NOT_SENT':'OUTCOME_UNKNOWN';this.commit({...attempt,errorCode:error.knownNotSent?'LOCAL_ABORT_BEFORE_TRANSPORT':'RECONCILIATION_REQUIRED'});if(error.knownNotSent)this.pending=null;throw error;}
 }
 observeOpened(fact){if(!this.pending?.intentId||fact.intentId!==this.pending.intentId||fact.wallet?.toLowerCase()!==this.plan.wallet.toLowerCase()||fact.instrumentId!==(this.plan.profile.asset==='BTC'?0:1)||!fact.positionId||!['account-SSE','provider-history'].includes(fact.source))throw new Error('EXACT_PROVIDER_EXECUTION_REQUIRED');const position={...fact,profileHash:this.hash,openingAttempt:this.pending.id};this.commit({kind:'PROVIDER_OPENED',...position});this.positions.set(String(fact.positionId),position);this.pending=null;return position;}
 async close(positionId){const p=this.positions.get(String(positionId));if(!p||!this.approval?.allowManagedClose)throw new Error('OWN_AUTHORIZED_POSITION_REQUIRED');if(p.closeAttempt)throw new Error('CLOSE_OUTCOME_PENDING_RECONCILE_FIRST');p.closeAttempt=crypto.randomUUID();this.commit({kind:'CLOSE_PREPARING',positionId:String(positionId),attemptId:p.closeAttempt,profileHash:this.hash});try{const r=await this.transport.close([String(positionId)],{onPrepared:m=>this.commit({kind:'CLOSE_PREPARED',positionId:String(positionId),attemptId:p.closeAttempt,intentId:m.intentId})});this.commit({kind:'CLOSE_ACK',positionId:String(positionId),attemptId:p.closeAttempt,intentId:r.intentId,uncertain:!!r.uncertain});return r;}catch(e){this.commit({kind:'CLOSE_OUTCOME_UNKNOWN',positionId:String(positionId),attemptId:p.closeAttempt});throw e;}}
}
module.exports={preflight,profileHash,LiveGateway};
