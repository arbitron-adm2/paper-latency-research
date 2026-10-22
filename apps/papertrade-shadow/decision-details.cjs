'use strict';
const crypto=require('node:crypto');
const key=row=>crypto.createHash('sha256').update(JSON.stringify([row.run_id??row.runId,Math.floor((row.second_ms??row.atMs)/1000)*1000,row.strategy,row.side,row.baseline_reason??row.baselineReason,row.timing_status??row.timingStatus,row.timing_reason??(row.timingReasons||[]).join('|'),row.entry_reason??row.entryReason??''])).digest('hex');
function capture(observer,c,atMs=observer.session.now){
 const guard=observer.guard.status(atMs),timing=guard.heuristic_timing||{},config=observer.guard.gate?.config||{};
 let native=null;try{native=observer.session.bridge.api.diagnosticSnapshot?.(c.strategy,c.side)||null;}catch{}
 return{schemaVersion:1,financialMode:'SIM',provenance:'EXACT_AT_OBSERVATION',observedAtMs:atMs,stage:c.stage||'RAW_CANDIDATE',
  sourceDecision:{ok:c.baselineOk??null,reason:c.baselineReason||null,notEveryPositiveEvaluationBecomesAnOrder:true},
  entryPolicy:{reason:c.entryReason||null,reachedSubmissionGate:c.stage==='FINAL_ENTRY_POLICY',evaluationMeaning:c.stage==='FINAL_ENTRY_POLICY'?'ACTUAL_VIRTUAL_SUBMISSION_ATTEMPT':'PREVIEW_NOT_A_SUBMISSION_ATTEMPT',nominalUsd:1260000,freeCapitalRaw:observer.session.engine.available().toString()},
  timing:{status:guard.status,reasons:[...(guard.reason_codes||[])],estimateKind:timing.estimate_kind||'UNKNOWN',entryEstimateMs:timing.entry_ms??null,exitEstimateMs:timing.exit_ms??null,
   budgets:{edgeLifetimeMs:config.signal_ttl_ms??null,entryMs:config.entry_budget_ms??null,exitMs:config.exit_budget_ms??null},
   pressureAgeMs:guard.pressure_freshness?.source_age_ms??null,pressureReceivedAgeMs:guard.pressure_freshness?.server_pressure_recv_age_ms??null,queueRevision:timing.evidence?.queue_revision??null,
   personalQueue:'UNKNOWN',calibration:'INCONCLUSIVE',economicPermission:false},
  input:{marketSequence:observer.session.lastInput?.sequence??null,guardCursor:guard.source?.cursor??null,bootId:observer.continuity.bootId,inputHealthy:observer.session.engine.inputHealthy,
   lastInvalidate:observer.session.lastInvalidate,recoveryUntilMs:observer.session.engine.integrityWarmUntil??null,lastTickChecks:observer.session.engine.lastInputChecks||null},
  native};
}
function stored(c,runId){return{schemaVersion:1,decisionKey:key({...c,runId}),observationCount:c.n||1,first:c.detailFirst||null,last:c.detailLast||c.detailFirst||null,
 aggregation:'first/last snapshots for this 1s/reason group; intermediate observations are counted, not individually reconstructed'};}
module.exports={decisionKey:key,captureDecisionDetails:capture,storedDecisionDetails:stored};
