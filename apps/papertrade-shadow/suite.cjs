'use strict';
const fs=require('node:fs'),path=require('node:path');
const C=require('./state-codec.cjs');
const {Execution}=require('./engine.cjs');
const {createBridge}=require('./strategy.cjs');
const {createNativeBridge}=require('./native-bridge.cjs');
const {captureEngine,restoreEngine,hasRisk,censor,resumeCensored,boundRuntime}=require('./engine-state.cjs');
const {decimal}=require('./money.cjs');
const {validateDefinition,catalog,MAX_MODELS}=require('./strategy-registry.cjs');
const {parameters:checkedParameters}=require('../strategy-lab/profiles.cjs');
const numericBudgets=new Set(['ENTRY_DELAY_BUDGET','EXIT_DELAY_BUDGET','EDGE_LIFETIME_BUDGET']);
const VLAD_PRESET_FILE=path.join(__dirname,'queue-pools/vlad-preset.json');
let vladPresetCache=null;
const PRESET_FILES={vlad:VLAD_PRESET_FILE,live:path.join(__dirname,'queue-pools/live-preset-20261020.json')};
const presetCache=new Map();
function executionPreset(name){
  if(presetCache.has(name))return presetCache.get(name);
  if(!PRESET_FILES[name])throw new Error('UNKNOWN_EXECUTION_PRESET');
  const v=JSON.parse(fs.readFileSync(PRESET_FILES[name],'utf8'));
  const ints=a=>Array.isArray(a)&&a.length>=8&&a.every(x=>Number.isSafeInteger(x)&&x>0&&x<120000);
  if(v.schemaVersion!==1||!ints(v.entryMs)||!Number.isSafeInteger(v.entryTtlMs)||v.entryTtlMs<3000||v.entryTtlMs>3600000||!Number.isSafeInteger(v.cancelMinLeadMs)||v.cancelMinLeadMs<0||v.cancelMinLeadMs>10000
    ||(v.cancelSuccessProbability!==undefined&&!(Number.isFinite(v.cancelSuccessProbability)&&v.cancelSuccessProbability>=0&&v.cancelSuccessProbability<=1))
    ||!Array.isArray(v.exitBuckets)||!v.exitBuckets.length||v.exitBuckets.some(b=>!(b.maxHoldMs===null||Number.isSafeInteger(b.maxHoldMs))||!Number.isSafeInteger(b.medianMs)||b.medianMs<=0||(b.values!==undefined&&!ints(b.values)))||v.exitBuckets[v.exitBuckets.length-1].maxHoldMs!==null
    ||!Number.isSafeInteger(v.fillFallbackMaxAgeMs)||v.fillFallbackMaxAgeMs<0||v.fillFallbackMaxAgeMs>60000||!Number.isSafeInteger(v.exitPriceRetryMs)||v.exitPriceRetryMs<0||v.exitPriceRetryMs>300000)throw new Error(name.toUpperCase()+'_PRESET_INVALID');
  const frozen=Object.freeze(v);presetCache.set(name,frozen);return frozen;
}
function vladPreset(){return executionPreset('vlad');}
function applyExecutionPreset(profile,config,scenario){
  if(!profile.execution)return;
  const name=profile.execution.preset,v=executionPreset(name);
  config.entryDeadline={...config.entryDeadline,signalTtlMs:v.entryTtlMs};
  Object.assign(scenario,{id:scenario.id+'+'+name+'-preset',entryMs:[...v.entryMs],congestion:0,volatility:0,volumeSlope:0,cancelMinLeadMs:v.cancelMinLeadMs,
    exitBuckets:v.exitBuckets.map(b=>({...b,...(b.values?{values:[...b.values]}:{})})),fillFallbackMaxAgeMs:v.fillFallbackMaxAgeMs,exitPriceRetryMs:v.exitPriceRetryMs});
  if(v.cancelSuccessProbability!==undefined)scenario.cancelSuccessProbability=v.cancelSuccessProbability;
}
// Strategy parameters may only replace existing source keys of the same type; sizing/LIVE/notification/learning keys are fixed.
const {FIXED_KEYS:FIXED_SOURCE_KEYS}=require('../strategy-lab/profiles.cjs');
function applySourceParameters(cfg,input){
  const patch=checkedParameters(input);
  for(const [k,v] of Object.entries(patch)){
    if(FIXED_SOURCE_KEYS.has(k))throw new Error('PARAMETER_FIXED:'+k);
    if(!Object.prototype.hasOwnProperty.call(cfg,k))throw new Error('PARAMETER_NOT_IN_SOURCE:'+k);
    if(typeof cfg[k]!==typeof v)throw new Error('PARAMETER_TYPE:'+k);
  }
  Object.assign(cfg,patch);
}
const PROFILES=Object.freeze([
  {id:'y2_timing_observe',asset:'BTC',family:'y2',timing:'observe',label:'Вчера 2 · timing предупреждает'},
  {id:'main_btc_observe',asset:'BTC',family:'main',timing:'observe',label:'Основной BTC · timing предупреждает'},
  {id:'main_btc_strict',asset:'BTC',family:'main',timing:'strict',label:'Основной BTC · строгий контроль'},
  {id:'native_eth_observe',asset:'ETH',family:'native',timing:'observe',label:'Исходный ETH · timing предупреждает'},
  {id:'native_eth_strict',asset:'ETH',family:'native',timing:'strict',label:'Исходный ETH · строгий контроль'},
]);
const schema=`
CREATE TABLE IF NOT EXISTS sim_strategy_archived(model_id TEXT PRIMARY KEY,archived_ms REAL NOT NULL,reason TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS sim_source_rearm(model_id TEXT PRIMARY KEY,trip_day_key REAL NOT NULL,requested_ms REAL NOT NULL,authorization TEXT NOT NULL,applied_ms REAL);
CREATE TABLE IF NOT EXISTS sim_strategy_definitions(model_id TEXT PRIMARY KEY,request_id TEXT NOT NULL UNIQUE,created_ms REAL NOT NULL,definition_json TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS sim_strategy_versions(model_hash TEXT PRIMARY KEY,model_id TEXT NOT NULL,created_ms REAL NOT NULL,profile_json TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS sim_strategy_accounts(model_id TEXT PRIMARY KEY,asset TEXT NOT NULL,family TEXT NOT NULL,timing_policy TEXT NOT NULL,model_hash TEXT NOT NULL,created_ms REAL NOT NULL,updated_ms REAL NOT NULL,unresolved INTEGER NOT NULL,snapshot_json TEXT NOT NULL,snapshot_hash TEXT NOT NULL,financial_mode TEXT NOT NULL DEFAULT 'SIM' CHECK(financial_mode='SIM'));
CREATE TABLE IF NOT EXISTS sim_strategy_events(model_id TEXT NOT NULL,sequence INTEGER NOT NULL,at_ms REAL NOT NULL,kind TEXT NOT NULL,payload_json TEXT NOT NULL,financial_mode TEXT NOT NULL DEFAULT 'SIM' CHECK(financial_mode='SIM'),PRIMARY KEY(model_id,sequence));
CREATE TABLE IF NOT EXISTS sim_strategy_trades(model_id TEXT NOT NULL,position_id TEXT NOT NULL,asset TEXT NOT NULL,opened_ms REAL NOT NULL,closed_ms REAL NOT NULL,cash_raw TEXT NOT NULL,payload_json TEXT NOT NULL,financial_mode TEXT NOT NULL DEFAULT 'SIM' CHECK(financial_mode='SIM'),PRIMARY KEY(model_id,position_id));
CREATE TABLE IF NOT EXISTS sim_strategy_decisions(model_id TEXT NOT NULL,second_ms INTEGER NOT NULL,strategy TEXT NOT NULL,side TEXT NOT NULL,source_ok INTEGER NOT NULL,source_reason TEXT NOT NULL,timing_status TEXT NOT NULL,timing_reason TEXT NOT NULL,entry_reason TEXT NOT NULL,observations INTEGER NOT NULL DEFAULT 1,PRIMARY KEY(model_id,second_ms,strategy,side,source_ok,source_reason,timing_status,timing_reason,entry_reason));
CREATE TABLE IF NOT EXISTS sim_signal_probes(id TEXT PRIMARY KEY,model_id TEXT NOT NULL,asset TEXT NOT NULL,at_ms REAL NOT NULL,strategy TEXT NOT NULL,side TEXT NOT NULL,entry_reason TEXT NOT NULL,initial_mid REAL NOT NULL,observations_json TEXT NOT NULL,meaning TEXT NOT NULL DEFAULT 'FIXED_HORIZON_PRICE_MOVEMENT_NOT_PNL');
CREATE INDEX IF NOT EXISTS sim_strategy_trades_time ON sim_strategy_trades(closed_ms);
CREATE INDEX IF NOT EXISTS sim_strategy_decisions_time ON sim_strategy_decisions(second_ms);
`;
function namespaced(db,id,prefix){
  const namespace='sim_native_'+id,sql=x=>String(x).replace(new RegExp('\\b'+prefix+'_[a-z_]+\\b','g'),n=>namespace+n.slice(prefix.length));
  return {exec:x=>db.exec(sql(x)),prepare:x=>db.prepare(sql(x)),tableName:namespace+'_trades'};
}
class Model{
  constructor(owner,profile,now){
    // Non-stop SIM: every comparison model is continuous (no quarantine/freeze); the preset only changes execution timing.
    this.owner=owner;this.profile=profile;this.id=profile.id;this.continuous=true;this.errors=0;this.lastError=null;this.errorUntil=0;this.gaps=0;this.lastGap=null;this.created=now;this.status='WARMUP';this.error=null;this.warnings=[];this.decisions=new Map();this.probes=[];this.probeLast=new Map();this.seenDecisions=new Map();this.lastTick=now;this.marketEvents=0;
    const base=owner.base,config=C.clone(base.config),experiment=C.clone(base.experiment),scenario=C.clone(base.scenario);
    applyExecutionPreset(profile,config,scenario);scenario.nonstop=true;
    if(config.executionPricePolicy){scenario.fillFallbackMaxAgeMs=0;scenario.exitPriceRetryMs=0;config.executionPricePolicy={...config.executionPricePolicy,missingPrice:'nonstop_entry_released_exit_waits_first_fresh_quote'};}
    config.priceScale=profile.asset==='ETH'?100:10;config.simProfile={asset:profile.asset,id:profile.id};
    const parameters=owner.parameters.instruments.find(r=>r[1]===profile.asset);scenario.instrumentRow=parameters;scenario.winFeeRaw=owner.parameters.winFeeRaw;config.bustBufferRaw=parameters[16];
    this.hash=C.hash(C.stringify({profile,parentModelHash:base.modelHash,parameters:owner.parameters,source:owner.nativeManifest}));
    this.engine=new Execution(config,experiment,scenario,now);const e=this.engine;let preSinkMark=e.ledger.length; // notes made below before noteSink exists are replayed to storage
    if(profile.family==='y2'){
      const privateDb=namespaced(owner.db,this.id,'btcy2_lag');e.persistentDb=privateDb;
      this.bridge=createBridge(e,base.launch,experiment,now);this.bridge.tableName=privateDb.tableName;
    }else this.bridge=createNativeBridge(e,{asset:profile.asset,namespace:'sim_native_'+profile.id},owner.db,now);
    if(profile.custom){this.bridge.api.cfg.spreadEnabled=profile.signals!=='book';this.bridge.api.cfg.bookEnabled=profile.signals!=='spread';}
    if(profile.parameters){applySourceParameters(this.bridge.api.cfg,profile.parameters);if(this.bridge.api.cfg.exitDev>=this.bridge.api.cfg.minDev||this.bridge.api.cfg.minDev>=this.bridge.api.cfg.maxDev)throw new Error('PARAMETER_RELATION');}
    e.bridge=this.bridge;e.inputHealthy=false;e.integrityWarmUntil=now+180000;
    const saved=owner.db.prepare('SELECT * FROM sim_strategy_accounts WHERE model_id=?').get(this.id);
    if(saved){
      this.created=saved.created_ms;
      if(C.hash(saved.snapshot_json)!==saved.snapshot_hash)throw new Error('MODEL_CHECKPOINT_CHECKSUM:'+this.id);
      const s=C.parse(saved.snapshot_json);restoreEngine(e,s.engine);preSinkMark=e.ledger.length;e.now=now;this.bridge.setTime(now);
      this.nativeRestore(s.native);this.probes=s.probes??[];this.probeLast=s.probeLast??new Map();
      // Non-stop SIM: restart, version change or a previously frozen account never stop the model.
      const wasCensored=!!e.accountingQuarantined||(e.suspendedTimers?.length>0)||[...e.positions.values()].some(p=>p.censored)||[...e.intents.values()].some(r=>r.state==='outcome_unknown');
      const exposure=hasRisk(e);
      if(wasCensored){const resumed=resumeCensored(e,now);this.resumed=resumed;e.note('nonstop_resume',{...resumed,previousReasons:s.engine.entryBlockReasons??[],policy:'censored/frozen SIM account resumed: suspended timers fire at first fresh HL mid, entries without timer released, exits requeued'});}
      else if(exposure)e.note('restart_with_exposure_continued',{policy:'continuous SIM: due timers fire after restart at first fresh HL mid; interval flagged, not frozen'});
      e.inputHealthy=false;e.integrityWarmUntil=now+180000;this.status='WARMUP';
      if(saved.model_hash!==this.hash)this.upgradedFrom=saved.model_hash;
    }
    if(saved)this.bindExecutionState(C.parse(saved.snapshot_json).native);
    if(e.executionPrices&&e.priceEpoch?.modelHash!==this.hash)e.startPriceEpoch(this.hash,now);
    owner.db.prepare('INSERT OR IGNORE INTO sim_strategy_versions VALUES(?,?,?,?)').run(this.hash,this.id,now,C.stringify({profile,parameters:owner.parameters,source:owner.nativeManifest,effectiveSourceConfig:this.bridge.api.cfg,simulationConfig:config,scenario}));
    if(this.upgradedFrom)owner.db.prepare('UPDATE sim_strategy_accounts SET model_hash=? WHERE model_id=?').run(this.hash,this.id);
    this.available=e.available();e.noteSink=r=>this.note(r);e.decisionSink=d=>this.decision(d);e.entryGate=()=>this.entryGate();
    for(const row of e.ledger.slice(preSinkMark))this.note(row); // e.g. nonstop_resume, restart_with_exposure_continued
    if(this.upgradedFrom)e.note('sim_model_version_changed',{previousModelHash:this.upgradedFrom,newModelHash:this.hash,cashPreserved:true,historyReset:false,openExposureContinued:hasRisk(e)});
    if(!saved)e.note('sim_model_created',{model:this.id,asset:profile.asset,initialVirtualCapital:'2300',notionalUsd:1260000,sourceDefaults:true});
    this.save(now);
  }
  nativeState(){return this.profile.family==='y2'?{cfg:this.bridge.api.cfg,guard:this.bridge.api.signalState().guard,positions:[...this.bridge.api.positions.values()]}:this.bridge.checkpointState();}
  nativeRestore(s){if(this.profile.family==='y2')this.bridge.api.restoreFlatPolicy(s);else this.bridge.restoreState(s);}
  bindExecutionState(native){
    const e=this.engine,api=this.bridge.api;
    api.pending.splice(0,api.pending.length,...[...e.intents.values()].filter(r=>r.kind==='entry'&&['submitted','queued','outcome_unknown'].includes(r.state)).map(r=>r.q));
    const saved=new Map((native?.positions??[]).map(p=>[String(p.positionId),p]));api.positions.clear();
    for(const p of e.positions.values()){
      let source=saved.get(String(p.positionId));
      const row=this.owner.db.prepare('SELECT * FROM "'+this.bridge.tableName+'" WHERE position_id=? AND status=\'open\'').get(p.positionId);
      if(!row||row.intent_id!==p.intentId||row.entry!==Number(p.entry)/Number(e.priceScale)||row.margin!==Number(p.margin)/1e18||row.side!==(p.long?'L':'S')||row.leverage!==p.leverage)throw new Error('SOURCE_POSITION_CHECKPOINT_MISMATCH:'+this.id);
      if(!source)source={...row,positionId:p.positionId,intentId:p.intentId,live:true,maxFav:row.max_fav??0,maxAdv:row.max_adv??0,bBest:NaN,bustUsd:NaN,legacyStateRestored:true};
      if(source.id!==row.id||source.entry!==row.entry||source.margin!==row.margin)throw new Error('SOURCE_POSITION_STATE_MISMATCH:'+this.id);
      const exit=[...e.intents.values()].find(r=>r.kind==='exit'&&r.positionId===p.positionId&&['queued','submitted','outcome_unknown'].includes(r.state));
      if(exit)source.closing={...(source.closing??{}),why:exit.reason,intentId:exit.id,sent:exit.createdAt,decidedAt:exit.createdAt};
      api.positions.set(source.id,source);
    }
  }
  snapshotState(){return {engine:captureEngine(this.engine),native:this.nativeState(),probes:this.probes,probeLast:this.probeLast};}
  save(now){if(this.archived)return;
    const blob=C.stringify(this.snapshotState());
    this.owner.db.prepare('INSERT INTO sim_strategy_accounts(model_id,asset,family,timing_policy,model_hash,created_ms,updated_ms,unresolved,snapshot_json,snapshot_hash) VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT(model_id) DO UPDATE SET updated_ms=excluded.updated_ms,unresolved=excluded.unresolved,snapshot_json=excluded.snapshot_json,snapshot_hash=excluded.snapshot_hash').run(this.id,this.profile.asset,this.profile.family,this.profile.timing,this.hash,this.created,now,hasRisk(this.engine)?1:0,blob,C.hash(blob));
  }
  note(row){
    row={...row,modelHash:this.hash};
    try{this.owner.history.transaction(()=>{
      this.owner.db.prepare('INSERT INTO sim_strategy_events(model_id,sequence,at_ms,kind,payload_json) VALUES(?,?,?,?,?)').run(this.id,row.sequence,row.time_ms,row.kind,C.stringify(row));
      if(row.kind==='close_fill')this.owner.db.prepare('INSERT INTO sim_strategy_trades(model_id,position_id,asset,opened_ms,closed_ms,cash_raw,payload_json) VALUES(?,?,?,?,?,?,?)').run(this.id,row.positionId,this.profile.asset,row.openedAt,row.closedAt,row.cash,C.stringify(row));
      this.save(row.time_ms);
    });
    if(['virtual_entry_limit','entry_fill','close_requested','close_fill'].includes(row.kind))this.owner.raw?.trigger('model:'+this.id+':'+row.kind,row.time_ms,{force:true});
    return true;}catch(error){
      try{const saved=this.owner.db.prepare('SELECT snapshot_json,snapshot_hash FROM sim_strategy_accounts WHERE model_id=?').get(this.id);if(saved&&C.hash(saved.snapshot_json)===saved.snapshot_hash)restoreEngine(this.engine,C.parse(saved.snapshot_json).engine,{preserveClock:true});}catch{}
      this.quarantine('MODEL_STORAGE:'+error.message);return false;}
  }
  quarantine(reason){
    // Non-stop SIM: a model error never freezes the account; record it and pause only new entries for 10 s.
    if(this.continuous){this.errors++;this.lastError={reason:String(reason).slice(0,200),atMs:this.engine.now};this.errorUntil=this.engine.now+10000;if(!this.inError&&!String(reason).startsWith('MODEL_STORAGE')){this.inError=true;try{this.engine.note('model_error_continued',{reason:this.lastError.reason,entriesPausedUntilMs:this.errorUntil});}catch{}finally{this.inError=false;}}return;}
    this.error=reason;this.status='QUARANTINED';censor(this.engine,reason);}
  entryGate(){
    const e=this.engine;if(this.error||e.accountingQuarantined)return{allowed:false,reason:'MODEL_QUARANTINED'};
    if(e.now<this.errorUntil)return{allowed:false,reason:'MODEL_ERROR_COOLDOWN'};
    if(!this.owner.storageOk())return{allowed:false,reason:'SHARED_STORAGE_UNHEALTHY'};
    if(!e.inputHealthy||e.now<e.integrityWarmUntil)return{allowed:false,reason:'INPUT_WARMUP_OR_GAP'};
    if(e.available()*95n<2100n*10n**18n*100n){
      // Non-stop comparison SIM: cash is restored to the initial virtual capital and the top-up is recorded; realised PnL (netUsdc) is untouched.
      if(hasRisk(e))return{allowed:false,reason:'FIXED_MARGIN_RESERVE'};
      const need=2300n*10n**18n-e.balance;if(need<=0n)return{allowed:false,reason:'FIXED_MARGIN_RESERVE'};
      e.balance+=need;this.topUps=(this.topUps??0)+1;this.topUpTotal=(this.topUpTotal??0n)+need;
      e.note('sim_virtual_topup',{amountUsdc:decimal(need),count:this.topUps,sessionTotalUsdc:decimal(this.topUpTotal),cashUsdc:decimal(e.balance),netUsdc:decimal(e.realised),policy:'comparison SIM only: virtual cash restored to 2300 so the model keeps trading; not a funded portfolio'});
    }
    if(hasRisk(e))return{allowed:false,reason:'ONE_OPEN_OR_PENDING_SLOT_LIMIT'};
    if(this.profile.execution?.preset)return{allowed:true,warning:['R3_NOT_APPLIED_'+this.profile.execution.preset.toUpperCase()+'_PRESET']};
    const g=this.owner.guard(),reasons=g.reason_codes??[];
    if(g.status==='ALLOW_SHADOW')return{allowed:true};
    if(this.profile.timing==='observe'&&reasons.length&&reasons.every(r=>numericBudgets.has(r)))return{allowed:true,warning:reasons};
    return{allowed:false,reason:'R3:'+reasons.join('|')};
  }
  decision(d){
    this.seenDecisions.set(d.strategy,[this.engine.now,!!d.ok,String(d.why??'')].join('|'));
    const g=this.owner.guard(),entry=this.engine.entryPolicyReason({margin:2100,asset:this.profile.asset})??'',reason=String(d.why??''),second=Math.floor(this.engine.now/1000)*1000;
    const row={second,strategy:d.strategy,side:d.side??'?',ok:!!d.ok,reason,timing:g.status,timingReason:(g.reason_codes??[]).join('|'),entry,n:1};
    const key=[second,row.strategy,row.side,row.ok,row.reason,row.timing,row.timingReason,row.entry].join('|'),old=this.decisions.get(key);
    if(old)old.n++;else if(this.decisions.size<256)this.decisions.set(key,row);
    if((!d.ok||entry)&&['L','S'].includes(d.side)&&this.engine.inputHealthy&&this.engine.now>=this.engine.integrityWarmUntil){
      const k=d.strategy+':'+d.side,last=this.probeLast.get(k)??0,f=this.bridge.api.feed.hl,mid=(f.bid+f.ask)/2;
      if(this.engine.now-last>=5000&&Number.isFinite(mid)&&this.probes.length<128){
        const p={id:this.id+':'+this.engine.now+':'+k,at:this.engine.now,strategy:d.strategy,side:d.side,reason:d.ok?entry:'SOURCE:'+reason,initialMid:mid,observations:[]};this.probes.push(p);this.probeLast.set(k,this.engine.now);
      }
    }
  }
  ingest(event,now,healthy){
    if(this.frozen||this.archived)return;
    this.engine.inputHealthy=this.continuous?true:healthy;if(event.kind!=='market')return;
    if(now-event.recv_ms>2500||now-event.recv_ms< -1000)return;
    if(event.source_ms!==null&&event.source_ms>event.recv_ms+1000){this.invalidate(now,'SOURCE_CLOCK_FUTURE');return;}
    if(this.profile.family==='y2'){if(event.symbol!=='BTC')return;this.advance(now,false);this.bridge.market(require('./live-feed.cjs').canonicalMarket(event));}
    else{if(event.symbol!==this.profile.asset&&!(this.profile.asset==='ETH'&&event.symbol==='BTC'&&event.venue==='bf'&&event.payload.frameKind==='quote'))return;this.advance(now,false);this.bridge.market(event);}
    this.marketEvents++;
  }
  advance(now,tick=true){
    if(this.frozen||this.archived)return;
    now=this.engine.executionPrices?.target(now)??now;
    const e=this.engine;if(now<e.now)return;
    while(e.timers[0]?.at<=now){const t=e.timers.shift();e.mark(Math.max(t.at,e.now));e.fire(t);} // overdue timers (after restart/resume) fire now, never in the past
    e.mark(now);
    if(tick&&now-this.lastTick>=100){
      this.bridge.step(now);this.lastTick=now;
      const cfg=this.bridge.api.cfg,live=!!(cfg?.live&&cfg?.enabled);if(live!==this.sourceLive){if(!live||this.sourceLive===false)this.sourceLiveChanged(live,now);this.sourceLive=live;}
      if(!live&&this.sourceOff)this.rearmSource(cfg,now);
      const candidates=this.bridge.api.rawCandidates?.()??this.bridge.api.observeCandidates?.()??[];
      for(const [strategy,d]of Object.entries(this.bridge.api.lastDecision))if(d?.t===now&&this.seenDecisions.get(strategy)!==[now,!!d.ok,String(d.why??'')].join('|')){
        const side=d.side??candidates.find(c=>c.strategy===strategy)?.side??'?';
        e.recordDecision(strategy,side,d.sig,!!d.ok,d);
      }
    }
    this.status=e.accountingQuarantined?'QUARANTINED':now<e.integrityWarmUntil?'WARMUP':this.sourceLive===false?'SOURCE_LIVE_OFF':'RUNNING';
    for(const p of this.probes)for(const seconds of[1,3,5,10,30])if(now>=p.at+seconds*1000&&!p.observations.some(x=>x.seconds===seconds)){
      const f=this.bridge.api.feed.hl,valid=!p.gap&&e.inputHealthy&&now-f.t<=1500&&now-(p.at+seconds*1000)<=1000;
      p.observations.push({seconds,observedAtMs:now,status:valid?'OBSERVED':'UNKNOWN',signedMoveBps:valid?(p.side==='L'?1:-1)*(((f.bid+f.ask)/2)/p.initialMid-1)*10000:null});
    }
  }
  rearmSource(cfg,now){
    // One-shot explicit user re-arm (sim_source_rearm) of a source switched off by its own day guard: only for the recorded trip day and only after the
    // source guard itself has reset on a later day. A later trip is never re-armed by the same request. Requests are read once per process.
    if(this.rearmRequest===undefined)this.rearmRequest=this.owner.db.prepare('SELECT trip_day_key,authorization FROM sim_source_rearm WHERE model_id=? AND applied_ms IS NULL').get(this.id)??null;
    const r=this.rearmRequest;if(!r||!cfg)return;
    const api=this.bridge.api,g=api.signalState?.().guard??api.state?.().guard??{};
    if(this.sourceOff.dayKey!==r.trip_day_key||g.tripped||!(g.dayKey>r.trip_day_key))return;
    cfg.live=true;cfg.enabled=true;this.rearmRequest=null;this.owner.db.prepare('UPDATE sim_source_rearm SET applied_ms=? WHERE model_id=?').run(now,this.id);
    this.engine.note('source_live_rearmed',{tripDayKey:r.trip_day_key,dayKey:g.dayKey,authorization:r.authorization,exposure:hasRisk(this.engine),
      policy:'one-shot explicit user re-arm after the source day guard reset; source parameters unchanged'});
  }
  sourceLiveChanged(live,now){
    // The source's own guard (e.g. Y2 day high-water drawdown) switched its LIVE branch off: entries stay suppressed. Recorded, never silent; no auto re-enable.
    const api=this.bridge.api,g=api.signalState?.().guard??api.state?.().guard??{};
    this.sourceOff=live?null:{atMs:now,reason:g.tripped?'SOURCE_DAY_HIGH_WATER_GUARD':'SOURCE_LIVE_OFF',dayKey:g.dayKey??null};
    this.engine.note(live?'source_live_enabled':'source_live_disabled',{cfgLive:!!api.cfg?.live,cfgEnabled:!!api.cfg?.enabled,guardTripped:!!g.tripped,dayKey:g.dayKey??null,dayPnlUsd:Number.isFinite(g.dayPnl)?g.dayPnl:null,exposure:hasRisk(this.engine),
      policy:'source own guard state, source parameters unchanged; comparison SIM entries suppressed while source LIVE is off; no automatic re-enable'});
  }
  invalidate(now,reason){if(this.archived)return;for(const p of this.probes)p.gap=true;
    if(this.continuous){this.gaps++;this.lastGap={atMs:now,reason};this.engine.note('input_gap_continued',{reason,exposure:hasRisk(this.engine),policy:'continuous SIM: source state retained; execution requires causal fresh HL quote, missing price retains unknown capital'});return;}
    this.bridge.invalidate(now,reason);if(hasRisk(this.engine))this.quarantine('INPUT_GAP_WITH_EXPOSURE');}
  flush(now){if(this.archived)return;
    this.owner.history.transaction(()=>{
      for(const d of this.decisions.values())this.owner.db.prepare('INSERT INTO sim_strategy_decisions VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT(model_id,second_ms,strategy,side,source_ok,source_reason,timing_status,timing_reason,entry_reason) DO UPDATE SET observations=observations+excluded.observations').run(this.id,d.second,d.strategy,d.side,d.ok?1:0,d.reason,d.timing,d.timingReason,d.entry,d.n);
      for(const p of this.probes)this.owner.db.prepare('INSERT INTO sim_signal_probes(id,model_id,asset,at_ms,strategy,side,entry_reason,initial_mid,observations_json) VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET observations_json=excluded.observations_json').run(p.id,this.id,this.profile.asset,p.at,p.strategy,p.side,p.reason,p.initialMid,JSON.stringify(p.observations));
    });this.decisions.clear();this.probes=this.probes.filter(p=>p.observations.length<5);boundRuntime(this.engine);
  }
  summary(){const e=this.engine;return{...this.profile,status:this.status,error:this.error,continuous:this.continuous,nonstop:true,errors:this.errors,lastError:this.lastError,resumed:this.resumed??null,sourceLive:this.sourceLive??null,sourceOff:this.sourceOff??null,virtualTopUps:this.topUps??0,virtualTopUpUsdc:decimal(this.topUpTotal??0n),gaps:this.gaps,lastGap:this.lastGap,archived:!!this.archived,archiveReason:this.archiveReason??null,modelHash:this.hash,frozenUnresolvedAccount:!!this.frozen,notionalUsd:1260000,priceScale:Number(e.priceScale),virtualCapitalUsdc:'2300',cashUsdc:decimal(e.balance),netUsdc:decimal(e.realised),reservedUsdc:decimal(e.reserved),lockedUsdc:decimal(e.locked),counts:e.counts,marketEvents:this.marketEvents,
    sourceParameters:this.profile.family==='y2'?'reviewed Y2 baseline':'original asset-specific source defaults; SIM sizing2100x600, max1, no auto-tune',
    executionPricePolicy:e.config.executionPricePolicy??null,priceEpoch:e.priceEpochSummary(),executionWatermarkMs:Number.isFinite(e.executionPrices?.watermarkMs)?e.executionPrices.watermarkMs:null,priceUnresolved:[...e.intents.values()].filter(r=>r.executionPriceMissing&&r.state==='outcome_unknown').length,
    latencyStatus:this.profile.asset==='ETH'?'BTC_LATENCY_TRANSFER_UNVALIDATED':'SCENARIO_NOT_PERSONAL_QUEUE',timingIsWarning:this.profile.timing==='observe',performance:'SIM_CONDITIONAL_NOT_VERIFIED_PROFIT',financialMutations:0};}
}
class StrategySuite{
  constructor({history,base,guard,storageOk,raw,now,allowFlatVersionUpgrade=false}){
    Object.assign(this,{history,db:history.db,base,guard,storageOk,raw,allowFlatVersionUpgrade});this.db.exec(schema);this.parameters=JSON.parse(fs.readFileSync(path.join(__dirname,'native-parameters.json'),'utf8'));this.nativeManifest=JSON.parse(fs.readFileSync(path.join(__dirname,'reviewed-native/manifest.json'),'utf8'));
    const custom=this.db.prepare('SELECT definition_json FROM sim_strategy_definitions ORDER BY created_ms,model_id').all().map(r=>JSON.parse(r.definition_json));
    this.models=[...PROFILES,...custom].map(p=>new Model(this,p,now));this.lastCheckpoint=now;this.lastFlush=now;
    // Archived models keep their saved account/history but no longer consume the shared feed (CPU budget).
    const archived=new Map(this.db.prepare('SELECT model_id,reason FROM sim_strategy_archived').all().map(r=>[r.model_id,r.reason]));
    for(const m of this.models)if(archived.has(m.id)){m.archived=true;m.status='ARCHIVED';m.archiveReason=archived.get(m.id);}
  }
  addStrategy(input,now){
    const profile=validateDefinition(input),prior=this.db.prepare('SELECT definition_json FROM sim_strategy_definitions WHERE request_id=?').get(input.requestId);
    if(prior){if(C.stringify(JSON.parse(prior.definition_json))!==C.stringify(profile))throw new Error('REQUEST_ID_ALREADY_USED');return{ok:true,replayed:true,model:this.models.find(m=>m.id===profile.id).summary()};}
    if(this.models.filter(m=>!m.archived).length+1>=MAX_MODELS)throw new Error('SHADOW_MODEL_LIMIT');
    if(!this.storageOk())throw new Error('SHARED_STORAGE_UNHEALTHY');
    let model;this.history.transaction(()=>{this.db.prepare('INSERT INTO sim_strategy_definitions VALUES(?,?,?,?)').run(profile.id,input.requestId,now,JSON.stringify(profile));model=new Model(this,profile,now);if(model.error)throw new Error(model.error);});
    this.models.push(model);return{ok:true,replayed:false,model:model.summary(),message:'Стратегия добавлена в shadow. Прогрев 3 минуты; общий поток, только SIM.'};
  }
  ingest(event,now,healthy){for(const m of this.models)try{m.ingest(event,now,healthy);}catch(error){m.quarantine(error.message);}}
  stageExecution(events,watermarkMs,{reset=false}={}){for(const m of this.models){if(m.archived)continue;const p=m.engine.executionPrices;if(!p)continue;if(reset)p.reset(watermarkMs);p.stage(events,watermarkMs);}}
  invalidate(now,reason){for(const m of this.models)m.invalidate(now,reason);}
  advance(now){for(const m of this.models)try{m.advance(now);}catch(error){m.quarantine(error.message);}
    if(now-this.lastFlush>=1000){for(const m of this.models)try{m.flush(now);}catch(error){m.quarantine(error.message);}this.lastFlush=now;}
    if(now-this.lastCheckpoint>=5000){for(const m of this.models)try{m.save(now);}catch(error){m.quarantine(error.message);}this.lastCheckpoint=now;}}
  stop(now){for(const m of this.models){m.status='STOPPED';m.flush(now);m.save(now);m.bridge.close();}}
  snapshot(){return{mode:'SIM',sharedFeed:true,externalSubscriptions:0,catalog:catalog(),models:this.models.map(m=>m.summary()),portfolioTotalsMeaning:'Independent experiments; balances/PnL must not be summed as one funded portfolio',markoutsMeaning:'Fixed-horizon price observations, NOT executable PnL'}};
}
function canFreezeUnresolvedModels(db){
 if(!db.prepare("SELECT 1 FROM sqlite_master WHERE name='sim_strategy_accounts'").get())return true;
 // Archived comparison models are frozen evidence and never advance: an unresolved archived account must not block a flat upgrade.
 const archived=db.prepare("SELECT 1 FROM sqlite_master WHERE name='sim_strategy_archived'").get();
 for(const row of db.prepare(archived?'SELECT snapshot_json,snapshot_hash FROM sim_strategy_accounts a WHERE a.unresolved>0 AND NOT EXISTS(SELECT 1 FROM sim_strategy_archived x WHERE x.model_id=a.model_id)':'SELECT snapshot_json,snapshot_hash FROM sim_strategy_accounts WHERE unresolved>0').all()){
  if(C.hash(row.snapshot_json)!==row.snapshot_hash)return false;const e=C.parse(row.snapshot_json).engine;
  if(!e.accountingQuarantined||[...e.positions.values()].some(p=>!p.censored)||[...e.intents.values()].some(r=>['submitted','queued'].includes(r.state)))return false;
 }return true;
}
function canPreserveLegacyPriceAccounts(db){
 // One-time price-model migration only. Preserve every comparison account and
 // its risk; never release capital just to make the primary upgrade look flat.
 if(!db.prepare("SELECT 1 FROM sqlite_master WHERE name='sim_strategy_accounts'").get())return true;
 for(const row of db.prepare('SELECT snapshot_json,snapshot_hash FROM sim_strategy_accounts a WHERE NOT EXISTS(SELECT 1 FROM sim_strategy_archived x WHERE x.model_id=a.model_id)').all()){
  if(C.hash(row.snapshot_json)!==row.snapshot_hash)return false;
  const e=C.parse(row.snapshot_json).engine;
  if(e.priceEpoch||typeof e.balance!=='bigint'||!(e.positions instanceof Map)||!(e.intents instanceof Map))return false;
  const reserved=[...e.intents.values()].filter(r=>r.kind==='entry'&&['submitted','queued','outcome_unknown'].includes(r.state)).reduce((n,r)=>n+r.margin,0n);
  const locked=[...e.positions.values()].reduce((n,p)=>n+p.margin,0n);
  if(e.reserved!==reserved||e.locked!==locked)return false;
 }return true;
}
function canContinueComparisonAccounts(db){
 // Non-stop comparison accounts are never frozen: a primary flat upgrade carries each one forward unchanged
 // (sim_model_version_changed, openExposureContinued). The upgrade itself releases nothing, so every active account
 // must be checksum-valid, not quarantined, and its reserved/locked must equal its own intents/positions.
 if(!db.prepare("SELECT 1 FROM sqlite_master WHERE name='sim_strategy_accounts'").get())return true;
 for(const row of db.prepare('SELECT snapshot_json,snapshot_hash FROM sim_strategy_accounts a WHERE NOT EXISTS(SELECT 1 FROM sim_strategy_archived x WHERE x.model_id=a.model_id)').all()){
  if(C.hash(row.snapshot_json)!==row.snapshot_hash)return false;
  const e=C.parse(row.snapshot_json).engine;
  if(e.accountingQuarantined||typeof e.balance!=='bigint'||!(e.positions instanceof Map)||!(e.intents instanceof Map))return false;
  const reserved=[...e.intents.values()].filter(r=>r.kind==='entry'&&['submitted','queued','outcome_unknown'].includes(r.state)).reduce((n,r)=>n+r.margin,0n);
  const locked=[...e.positions.values()].reduce((n,p)=>n+p.margin,0n);
  if(e.reserved!==reserved||e.locked!==locked)return false;
 }return true;
}
module.exports={StrategySuite,Model,PROFILES,schema,numericBudgets,canFreezeUnresolvedModels,canPreserveLegacyPriceAccounts,canContinueComparisonAccounts};
