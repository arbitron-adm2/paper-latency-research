'use strict';
const fs=require('node:fs'),path=require('node:path');
const {DatabaseSync}=require('node:sqlite');
const C=require('../papertrade-shadow/state-codec.cjs');
const {ShadowProcess}=require('./shadow-process.cjs');
const {catalog}=require('../papertrade-shadow/strategy-registry.cjs');
const ROOT=path.resolve(__dirname,'../..'),STATE=path.join(ROOT,'reports/persistent-shadow-state');
const SERVICE=path.join(ROOT,'apps/papertrade-shadow/service.cjs');
function createShadowRuntime(){
 return new ShadowProcess({root:ROOT,script:SERVICE,args:['start','--state-dir',STATE],sourceHash:'0512ea94e8eb4ae9553719a15fbf4578955611d7ee8d7c424e7188faf2a46674'});
}
let cached=null,cachedAt=0;
function historyView(limit=40,now=Date.now()){
 if(!Number.isSafeInteger(limit)||limit<1||limit>100)throw new Error('INVALID_HISTORY_LIMIT');
 if(cached&&now-cachedAt<1500&&cached.limit===limit)return cached;
 const file=path.join(STATE,'shadow-history.sqlite');
 if(!fs.existsSync(file))return{mode:'SIM',status:'INITIALISING',notionalUsd:1260000,trades:[],positions:[],intents:[],decisions:[],runs:[]};
 if(fs.lstatSync(file).isSymbolicLink()||!fs.realpathSync(file).startsWith(fs.realpathSync(STATE)+path.sep))throw new Error('INVALID_HISTORY_PATH');
 const db=new DatabaseSync(file,{readOnly:true});
 try{
  db.exec('PRAGMA query_only=ON');db.exec('PRAGMA busy_timeout=1000');
  const runs=db.prepare('SELECT run_id,created_ms,last_ms,status,restarts,model_hash FROM sim_runs ORDER BY created_ms DESC LIMIT 10').all();
  const runId=runs[0]?.run_id;
  const decode=row=>{try{return C.parse(row.payload_json);}catch{return null;}};
  const trades=db.prepare('SELECT run_id,position_id,opened_ms,closed_ms,entry_raw,exit_raw,margin_raw,leverage,cash_raw,fee_raw,reason,financial_mode FROM sim_trades ORDER BY closed_ms DESC LIMIT ?').all(limit);
  const positionRows=runId?db.prepare("SELECT position_id,status,opened_ms,closed_ms,margin_raw,payload_json FROM sim_positions WHERE run_id=? ORDER BY opened_ms DESC LIMIT ?").all(runId,limit):[];
  const positions=positionRows.filter(row=>row.status!=='closed').map(row=>{const p=decode(row)||{};return{positionId:row.position_id,status:row.status,openedMs:row.opened_ms,side:p.long?'L':p.long===false?'S':null,entryRaw:p.entry==null?null:String(p.entry),marginRaw:row.margin_raw,leverage:p.leverage??null,financialMode:'SIM'};});
  const intentRows=runId?db.prepare('SELECT intent_id,kind,state,created_ms,notional_raw,deadline_sec,payload_json FROM sim_intents WHERE run_id=? ORDER BY created_ms DESC LIMIT ?').all(runId,limit):[];
  const intents=intentRows.map(row=>{const r=decode(row)||{};return{intentId:row.intent_id,kind:row.kind,state:row.state,createdMs:row.created_ms,notionalRaw:row.notional_raw,deadlineSec:row.deadline_sec,side:r.q?.side??null,reason:r.invalid?.reason??null,financialMode:'SIM'};});
  const decisions=runId?db.prepare('SELECT id,second_ms,strategy,side,raw_trigger,baseline_ok,baseline_reason,timing_status,timing_reason,entry_reason,observations,signal_min,signal_max FROM sim_decisions WHERE run_id=? ORDER BY second_ms DESC LIMIT ?').all(runId,limit):[];
  for(const row of trades)if(row.financial_mode!=='SIM')throw new Error('NON_SIM_HISTORY_REFUSED');
  const totals={trades:Number(db.prepare('SELECT count(*) n FROM sim_trades').get().n),intents:Number(db.prepare('SELECT count(*) n FROM sim_intents').get().n),decisions:Number(db.prepare('SELECT count(*) n FROM sim_decisions').get().n),events:Number(db.prepare('SELECT count(*) n FROM sim_events').get().n),marketSeconds:Number(db.prepare('SELECT count(*) n FROM market_seconds').get().n),forecasts:Number(db.prepare('SELECT count(*) n FROM sim_forecasts').get().n),publicEvidence:Number(db.prepare('SELECT count(*) n FROM public_evidence').get().n)};
  let models=[],modelTrades=[],modelDecisions=[],modelPositions=[],signalProbes=[];
  if(db.prepare("SELECT 1 FROM sqlite_master WHERE name='sim_strategy_accounts'").get()){
   let telemetry;try{telemetry=JSON.parse(fs.readFileSync(path.join(STATE,'telemetry.json'),'utf8'));}catch{}
   models=[...(telemetry?.comparison?.models??[])];
   const primary=telemetry?.assets?.BTC?.simulated;if(primary)models.unshift({id:'y2_strict_primary',label:'Вчера 2 · строгий контроль',asset:'BTC',status:telemetry.acceptance?.completeWarmup?telemetry.status:telemetry.status==='RUNNING'?'WARMUP':telemetry.status,counts:primary.counts,priceEpoch:primary.priceEpoch,executionPricePolicy:primary.executionPricePolicy,cashUsdc:String(Number(primary.available_scenario)+Number(primary.reserved_scenario)+Number(primary.locked_scenario)),netUsdc:primary.net_pnl_scenario,reservedUsdc:primary.reserved_scenario,lockedUsdc:primary.locked_scenario,latencyStatus:'SCENARIO_NOT_PERSONAL_QUEUE',timingIsWarning:false});
   for(const row of db.prepare('SELECT model_id,asset,snapshot_json FROM sim_strategy_accounts').all()){
    const value=C.parse(row.snapshot_json);for(const p of value.engine.positions.values())modelPositions.push({modelId:row.model_id,asset:row.asset,positionId:p.positionId,side:p.long?'L':'S',entryRaw:String(p.entry),openedMs:p.openedAt,censored:!!p.censored});
   }
   modelTrades=db.prepare('SELECT model_id,asset,opened_ms,closed_ms,payload_json FROM sim_strategy_trades ORDER BY closed_ms DESC LIMIT ?').all(limit).map(row=>{const p=C.parse(row.payload_json);return{modelId:row.model_id,asset:row.asset,openedMs:row.opened_ms,closedMs:row.closed_ms,positionId:p.positionId,side:p.long?'L':'S',entryRaw:p.entry_raw,exitRaw:p.exit_raw,marginRaw:p.margin_raw,leverage:p.leverage,cashRaw:p.cash,reason:p.reason,financialMode:'SIM'};});
   modelDecisions=db.prepare('SELECT model_id,second_ms,strategy,side,source_ok,source_reason,timing_status,timing_reason,entry_reason,observations FROM sim_strategy_decisions ORDER BY second_ms DESC LIMIT ?').all(limit);
   signalProbes=db.prepare('SELECT model_id,asset,at_ms,strategy,side,entry_reason,observations_json FROM sim_signal_probes ORDER BY at_ms DESC LIMIT ?').all(Math.min(limit,20)).map(row=>({...row,observations:JSON.parse(row.observations_json),observations_json:undefined}));
  }
  let blockSummary=[],modelIntents=[];
  if(models.length){
   blockSummary=db.prepare("SELECT model_id,CASE WHEN source_ok=0 THEN 'SOURCE:'||source_reason ELSE entry_reason END reason,SUM(observations) observations,MAX(second_ms) lastMs FROM sim_strategy_decisions WHERE second_ms>=? AND (source_ok=0 OR entry_reason<>'') GROUP BY 1,2 ORDER BY observations DESC LIMIT 60").all(now-600000);
   for(const row of db.prepare('SELECT model_id,asset,snapshot_json FROM sim_strategy_accounts').all()){
    const value=C.parse(row.snapshot_json);for(const r of [...value.engine.intents.values()].slice(-10))modelIntents.push({modelId:row.model_id,asset:row.asset,intentId:r.id,kind:r.kind,state:r.state,createdMs:r.createdAt,side:r.q?.side,reason:r.invalid?.reason??null});
   }
  }
  cached={mode:'SIM',status:'READY',atMs:now,limit,notionalUsd:1260000,runId:runId??null,totals,runs,trades,positions,intents,decisions,models,modelTrades,modelDecisions,modelPositions,signalProbes,catalog:catalog(),blockSummary,modelIntents,realOrders:0,warning:'Старые результаты SIM содержат исполнения по устаревшей цене. Новая серия после исправления цены показана отдельно. Независимые SIM-модели: результаты и виртуальные капиталы не складываются в один портфель. Очередь и выплаты — условные модели; движение после отказа не является PnL.'};cachedAt=now;return cached;
 }finally{db.close();}
}
function clearHistoryCache(){cached=null;cachedAt=0;}
module.exports={createShadowRuntime,historyView,clearHistoryCache,STATE,SERVICE};
