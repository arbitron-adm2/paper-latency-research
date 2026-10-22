'use strict';
// Deliberately no conversion from holding time / market move timing to queue delay.
function classifyInterval(x){
  if(!x||!['BTC','ETH'].includes(x.asset)||!['own','Vlad','public-other'].includes(x.owner))throw new Error('Interval owner and asset scope required');
  const base={owner:x.owner,asset:x.asset,margin:x.margin??null,leverage:x.leverage??null,notional:x.notional??null,
    source:x.source??null,censored:!!x.censored,ambiguity:x.ambiguity??null};
  if(x.submittedAtMs!=null&&x.executedAtMs!=null&&x.intentId&&x.strongIntentAttribution===true&&!x.ambiguity&&!x.censored){
    if(x.executedAtMs<x.submittedAtMs)throw new Error('Negative paired latency');
    return {...base,category:'factual-paired-submit-to-fill',intervalMs:x.executedAtMs-x.submittedAtMs,
      intervalKind:'submit-to-fill',queueCalibrationEligible:true,transferLimit:'Same owner/asset/size/market regime only; transfer to other sizes is an assumption'};
  }
  if(x.openedAtMs!=null&&x.closedAtMs!=null){
    return {...base,category:'factual-holding-period',intervalMs:x.closedAtMs-x.openedAtMs,intervalKind:'holding-period',queueCalibrationEligible:false};
  }
  if(x.decisionAtMs!=null&&x.executedAtMs!=null){
    return {...base,category:'empirical-journal-transfer-advisory',intervalMs:x.executedAtMs-x.decisionAtMs,
      intervalKind:'decision-to-execution; send/attribution uncertain',queueCalibrationEligible:false};
  }
  return {...base,category:x.censored?'censored-unknown':'unidentified',intervalMs:null,intervalKind:'unknown',queueCalibrationEligible:false};
}
function scopedEvidence(intervals,target){
  const classified=intervals.map(classifyInterval);
  return {target,intervals:classified,eligibleExactScope:classified.filter(x=>x.queueCalibrationEligible&&x.owner===target.owner&&x.asset===target.asset&&x.notional===target.notional).length,
    objectiveQueueModel:'UNIDENTIFIABLE',rank:null,
    classes:{factual:'Verified raw executions/paired identities only',empiricalTransfer:'Own/Vlad scoped intervals, never cross-asset/size without flag',pureHypothesis:'Conflict rates, congestion, size priority and unobserved tails'},
    financialPerformance:'simulated scenario, not observed return'};
}
module.exports={classifyInterval,scopedEvidence};
