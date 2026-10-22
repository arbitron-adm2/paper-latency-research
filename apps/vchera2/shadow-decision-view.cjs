'use strict';
const fs=require('node:fs'),path=require('node:path');const {DatabaseSync}=require('node:sqlite');
const C=require('../papertrade-shadow/state-codec.cjs');const {decisionKey}=require('../papertrade-shadow/decision-details.cjs');const {FeedGuard}=require('../paper-queue-latency/src/feed-guard.cjs');
const ROOT=path.resolve(__dirname,'../..'),STATE=path.join(ROOT,'reports/persistent-shadow-state');
function open(){const file=path.join(STATE,'shadow-history.sqlite');if(fs.lstatSync(file).isSymbolicLink()||!fs.realpathSync(file).startsWith(fs.realpathSync(STATE)+path.sep))throw new Error('INVALID_HISTORY_PATH');const db=new DatabaseSync(file,{readOnly:true});db.exec('PRAGMA query_only=ON');db.exec('PRAGMA busy_timeout=1000');return db;}
function decisionDetail(id){
 if(!Number.isSafeInteger(id)||id<1)throw new Error('INVALID_DECISION_ID');const db=open();
 try{
  const row=db.prepare('SELECT * FROM sim_decisions WHERE id=?').get(id);if(!row)return{mode:'SIM',status:'NOT_FOUND'};
  const k=decisionKey(row),stored=db.prepare("SELECT payload_json FROM quality_events WHERE run_id=? AND kind='DECISION_DETAILS' AND at_ms>=? AND at_ms<? ORDER BY id DESC LIMIT 100").all(row.run_id,row.second_ms,row.second_ms+1000);
  for(const item of stored){const detail=C.parse(item.payload_json);if(detail.decisionKey===k)return{mode:'SIM',status:'EXACT_RECORDED_DETAILS',row,details:detail,actualOrders:0};}
  const f=db.prepare('SELECT at_ms,sequence,model_hash,input_hash,payload_json FROM sim_forecasts WHERE run_id=? AND at_ms<=? ORDER BY at_ms DESC LIMIT 1').get(row.run_id,row.second_ms);
  const p=f?C.parse(f.payload_json):null,g=new FeedGuard({runId:'readonly-historical-explanation'}),sameConfig=!!f&&f.model_hash===g.modelHash;
  return{mode:'SIM',status:'LEGACY_PARTIAL_EXPLANATION',row,details:{provenance:'NEAREST_PRECEDING_FORECAST_NOT_EXACT_DECISION_STATE',historicalMeasurementsStored:false,
   sourceDecision:{ok:row.baseline_ok,reason:row.baseline_reason,meaning:row.baseline_ok===null?'RAW_TRIGGER_NOT_FULL_BASELINE_PERMISSION':'Recorded pre-queue evaluation; later native queue filters may still reject'},
   native:null,timing:p?{observedAtMs:f.at_ms,ageBeforeDecisionMs:row.second_ms-f.at_ms,sequence:f.sequence,inputHash:f.input_hash,estimateKind:p.heuristic_timing?.estimate_kind,entryEstimateMs:p.heuristic_timing?.entry_ms??null,exitEstimateMs:p.heuristic_timing?.exit_ms??null,
    budgets:sameConfig?{edgeLifetimeMs:g.gate.config.signal_ttl_ms,entryMs:g.gate.config.entry_budget_ms,exitMs:g.gate.config.exit_budget_ms}:null,budgetsVerifiedAgainstForecastHash:sameConfig,reasons:p.heuristic_timing?.reasons||[],personalQueue:'UNKNOWN'}:null,
   warning:'Точные старые значения не записаны. Предыдущий forecast не выдается за snapshot именно этой попытки. Новые решения сохраняют first/last measurements.'},actualOrders:0};
 }finally{db.close();}
}
function diagnosticSummary(){const db=open();try{return{mode:'SIM',notionalUsd:1260000,observationsAreNotUniqueTrades:true,
 timing:db.prepare('SELECT timing_status,timing_reason,count(*) rows,sum(observations) observations FROM sim_decisions GROUP BY 1,2 ORDER BY observations DESC').all(),
 source:db.prepare('SELECT baseline_ok,baseline_reason,count(*) rows,sum(observations) observations FROM sim_decisions GROUP BY 1,2 ORDER BY observations DESC LIMIT 20').all(),
 attempts:db.prepare("SELECT count(*) n FROM sim_events WHERE kind='entry_not_submitted'").get().n,
 quality:db.prepare('SELECT kind,count(*) n FROM quality_events GROUP BY kind ORDER BY n DESC LIMIT 15').all(),actualOrders:0};}finally{db.close();}}
module.exports={decisionDetail,diagnosticSummary};
