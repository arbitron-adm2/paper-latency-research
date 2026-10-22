'use strict';
const C=require('./state-codec.cjs');
const {IncrementalSession}=require('./incremental.cjs');
const {Continuity,validateEnvelope,ENDPOINT}=require('./live-feed.cjs');
const {captureEngine,restoreEngine,hasRisk,censor,boundRuntime}=require('./engine-state.cjs');
const {loadR3,captureGuard,restoreGuard}=require('./r3-observer.cjs');
const {History}=require('./history.cjs');
const {RawRetention}=require('./raw-retention.cjs');
const {buildProfile}=require('./persistent-profile.cjs');
const {StrategySuite,canFreezeUnresolvedModels,canPreserveLegacyPriceAccounts,canContinueComparisonAccounts}=require('./suite.cjs');
const {captureDecisionDetails,storedDecisionDetails}=require('./decision-details.cjs');
class PersistentObserver{
  constructor({stateDir,clock=Date.now,historyFactory=dir=>new History(dir,{now:clock()}),comparisonEnabled=false,flatUpgrade=false}={}){
    this.clock=clock;this.profile=buildProfile();this.history=historyFactory(stateDir);this.dir=this.history.dir;
    this.startedAt=clock();this.segmentInfo=this.history.begin(this.profile,this.startedAt);this.runId=this.history.runId;
    this.status='STARTING';this.failure=null;this.storage={healthy:true,dbBlocked:false,warnings:[],historyLoss:0,rawLoss:0};
    this.market=new Map();this.decisions=new Map();this.forecasts=[];this.evidence=[];this.contextSeen=new Map();
    this.lastFlush=this.startedAt;this.lastCheckpoint=this.startedAt;this.lastRollup=this.startedAt;this.checkpointInfo=null;this.lastObservedAtMs=null;this.connection=null;
    this.stats={batches:0,events:0,pressure:0,publicTrades:0,market:0,rawCandidates:0,sourceDecisions:0,timingSuppressed:0,requests:0};
    this.continuity=new Continuity();this.R3=loadR3();this.guardEpoch=0;this.guard=this.makeGuard();
    const recovered=this.history.recovery();this.recoveryRecord=recovered;this.requiresHandshake=!this.segmentInfo.newAccount;
    const origin=recovered.state?.session?.startMs??this.segmentInfo.previous?.created_ms??this.startedAt;
    this.session=new IncrementalSession(this.profile.config,this.profile.launch,this.profile.experiment,this.profile.scenario,origin,{persistentDb:this.history.db});
    this.history.installNativeEpoch();
    if(recovered.state?.continuity)Object.assign(this.continuity,C.clone(recovered.state.continuity));
    this.vmExact=false;this.modelCompatible=!recovered.modelHash||recovered.modelHash===this.profile.modelHash;
    this.recovery={mode:this.segmentInfo.newAccount?'NEW_VIRTUAL_ACCOUNT':'AWAITING_FEED_PROOF',reason:null,requiresExplicitRecovery:false,censored:false,restoredWarmup:false};
    if(recovered.state&&this.modelCompatible){
      try{this.restore(recovered.state);this.vmExact=recovered.state.vmStateProven!==false;}catch(e){this.recovery.reason='VM_RESTORE_FAILED:'+e.message;}
    }
    const e=this.session.engine;
    this.flatUpgradeApplied=false;
    if(flatUpgrade){
      const checked=recovered.state?.session?.engine;
      const safe=checked&&(checked.ledgerSequence??0)>=(recovered.safety?.ledgerSequence??0)?checked:recovered.safety;
      const comparisonSafe=canFreezeUnresolvedModels(this.history.db)||(this.profile.config.executionPricePolicy&&canPreserveLegacyPriceAccounts(this.history.db))||canContinueComparisonAccounts(this.history.db);
      if(this.segmentInfo.newAccount||this.modelCompatible||!recovered.clean||!recovered.state||recovered.corrupt?.length||recovered.state.storage?.dbBlocked||recovered.state.storage?.historyLoss||!safe||safe.reserved!==0n||safe.locked!==0n||safe.positions.size||[...safe.intents.values()].some(r=>['submitted','queued','outcome_unknown'].includes(r.state))||!comparisonSafe)throw new Error('FLAT_PROFILE_UPGRADE_REQUIRES_CLEAN_FLAT_PRIOR_MODEL');
      restoreEngine(e,safe);e.now=this.startedAt;e.inputHealthy=false;e.accountingQuarantined=false;e.entryBlockReasons=[];e.integrityWarmUntil=this.startedAt+180000;
      this.session.now=this.startedAt;this.session.nextTick=this.startedAt;this.session.bridge.setTime(this.startedAt);
      const native=recovered.state?.session?.bridge?.native;if(native)this.session.bridge.api.restoreFlatPolicy({cfg:native.cfg,guard:native.objects.guard});
      this.continuity=new Continuity();this.requiresHandshake=false;this.vmExact=true;this.modelCompatible=true;this.flatUpgradeApplied=true;
      this.recovery={mode:'EXPLICIT_FLAT_MODEL_UPGRADE',reason:null,requiresExplicitRecovery:false,censored:false,restoredWarmup:false};
      this.history.transaction(()=>{this.history.db.prepare('UPDATE sim_runs SET model_hash=?,config_hash=? WHERE run_id=?').run(this.profile.modelHash,this.profile.configHash,this.runId);this.history.quality('EXPLICIT_FLAT_MODEL_UPGRADE',{previousModelHash:recovered.modelHash,newModelHash:this.profile.modelHash,cashPreserved:true,historyPreserved:true,inputsRewarm:true},this.startedAt);});
    }
    if(!this.segmentInfo.newAccount&&!this.flatUpgradeApplied){
      const savedSequence=recovered.state?.session?.engine?.ledgerSequence??0;
      if(recovered.safety&&(!this.vmExact||(recovered.safety.ledgerSequence??0)>savedSequence)){
        restoreEngine(e,recovered.safety);this.session.now=e.now;this.session.bridge.setTime(e.now);this.vmExact=false;
      }
      this.session.bridge.api.bindPending(e.intents);
      if(!this.modelCompatible)this.recovery.reason='MODEL_VERSION_MISMATCH';
      else if(recovered.corrupt?.length)this.recovery.reason='CORRUPTED_CHECKPOINT';
      else if(!recovered.state)this.recovery.reason=recovered.reason??'CHECKPOINT_MISSING';
      if(!this.vmExact||this.recovery.reason){censor(e,this.recovery.reason??'INEXACT_RECOVERY');this.recovery.censored=hasRisk(e);this.recovery.requiresExplicitRecovery=true;}
    }
    this.raw=new RawRetention(this.history,this.profile.rawPolicy,{onWarning:code=>this.rawWarning(code),onRecovered:()=>this.rawRecovered()});
    if(recovered.state?.raw)this.raw.restore(recovered.state.raw);
    e.noteSink=row=>this.onNote(row);
    e.decisionSink=decision=>this.onDecision(decision);
    e.entryGate=()=>this.entryGate();
    if(this.flatUpgradeApplied){e.note('research_model_version_changed',{cashRaw:e.balance.toString(),previousModelHash:recovered.modelHash,newModelHash:this.profile.modelHash,historyReset:false});this.saveCheckpoint(false,this.startedAt);}
    if(this.segmentInfo.newAccount){e.note('virtual_account_initialized',{initialCashRaw:e.balance.toString(),notionalUsd:1260000,topUps:false,financialPerformance:'SIM'});this.vmExact=true;this.saveCheckpoint(false,this.startedAt);}
    this.suite=comparisonEnabled?new StrategySuite({history:this.history,base:this.profile,allowFlatVersionUpgrade:this.flatUpgradeApplied,guard:()=>this.guard.status(this.session.now),storageOk:()=>!this.storage.dbBlocked /* comparison SIM models stop only on durable DB failure; raw-context warnings are reported, not blocking */,raw:this.raw,now:this.startedAt}):null;
    this.cachedTotals=this.history.totals();this.status=this.requiresHandshake?'RESUMING':'RUNNING';
  }
  makeGuard(){return new this.R3.FeedGuard({runId:this.runId+':timing:'+this.guardEpoch,selector:{asset:'BTC',notional_usd:1260000},onPrediction:p=>this.onPrediction(p)});}
  safeWrite(fn){
    if(this.storage.dbBlocked){this.storage.historyLoss++;return false;}
    try{this.history.transaction(fn);return true;}catch(e){this.storageFailure(e);return false;}
  }
  storageFailure(error){
    if(this.storage.dbBlocked)return;
    this.storage.healthy=false;this.storage.dbBlocked=true;this.storage.warnings.push('DB:'+error.message);this.storage.warnings=this.storage.warnings.slice(-32);
    if(this.raw)this.raw.disabled=true;
    if(this.session){
      const e=this.session.engine;
      if(this.history.lastSafety)restoreEngine(e,this.history.lastSafety,{preserveClock:true});
      censor(e,'DURABLE_STORAGE_ERROR');this.session.bridge.api.bindPending(e.intents);
      this.vmExact=false;this.recovery.requiresExplicitRecovery=true;this.recovery.censored=hasRisk(e);this.recovery.reason='DURABLE_STORAGE_ERROR';
    }
  }
  rawWarning(code){
    this.storage.healthy=false;this.storage.rawLoss++;if(!this.storage.warnings.includes(code))this.storage.warnings.push(code);this.storage.warnings=this.storage.warnings.slice(-32);
    if(this.session){const e=this.session.engine;e.entryBlockReasons=[...new Set([...(e.entryBlockReasons??[]),'RAW_STORAGE_WARNING'])];}
    // Raw-context loss blocks the strict primary only while the store is unhealthy; it no longer demands an explicit recovery handshake.
  }
  rawRecovered(){
    this.storage.healthy=!this.storage.dbBlocked;this.storage.warnings=this.storage.warnings.filter(w=>!w.startsWith('RAW_'));
    if(this.session){const e=this.session.engine;e.entryBlockReasons=(e.entryBlockReasons??[]).filter(r=>r!=='RAW_STORAGE_WARNING');}
  }
  onNote(row){
    const attempt=row.kind==='entry_not_submitted'?{...row,diagnostics:captureDecisionDetails(this,{strategy:row.strategy,side:row.side,entryReason:row.reason,stage:'FINAL_ENTRY_POLICY'})}:row;
    const committed=this.safeWrite(()=>this.history.appendNote(attempt,captureEngine(this.session.engine)));
    if(['virtual_entry_limit','entry_fill','close_requested','close_fill','entry_not_submitted'].includes(row.kind)){
      try{this.raw?.trigger('financial:'+row.kind+':'+(row.intentId??row.positionId??row.reason),this.session.engine.now,{force:row.kind!=='entry_not_submitted'});}catch(e){this.rawWarning(e.message);}
    }
    return committed;
  }
  onPrediction(p){
    const minimal={...p};delete minimal.input_pressure;
    if(this.forecasts.length>=512){this.rawWarning('PREDICTION_BUFFER_BOUND');this.storage.historyLoss++;return;}
    this.forecasts.push(minimal);
  }
  entryGate(){
    if(this.requiresHandshake||this.recovery.requiresExplicitRecovery||this.storage.dbBlocked)return {allowed:false,reason:'PERSISTENT_RECOVERY_OR_STORAGE_QUARANTINE'};
    if(!this.session.bridge.api.cfg.live||!this.session.bridge.api.cfg.enabled)return {allowed:false,reason:'SOURCE_STOP_LATCHED'};
    if(this.session.engine.available()*95n<2100n*10n**18n*100n)return {allowed:false,reason:'VIRTUAL_CAPITAL_BELOW_FIXED_PRESET_RESERVE'};
    if(this.session.engine.positions.size||[...this.session.engine.intents.values()].some(r=>['submitted','queued','outcome_unknown'].includes(r.state)))return {allowed:false,reason:'ONE_OPEN_OR_PENDING_SLOT_LIMIT'};
    const status=this.guard.status(this.session.now);
    return status.status==='ALLOW_SHADOW'?{allowed:true,meaning:'TIMING_ONLY_NOT_ECONOMIC_OR_LIVE'}:{allowed:false,reason:'R3:'+status.reason_codes.join('|')};
  }
  addDecision(c){
    if(this.decisions.size>=256){this.rawWarning('DECISION_BUFFER_BOUND');this.storage.historyLoss++;return;}
    const now=this.session.now,key=Math.floor(now/1000)+'|'+c.strategy+'|'+c.side+'|'+c.baselineReason+'|'+c.entryReason+'|'+c.timingReasons.join('|');
    const detail=captureDecisionDetails(this,c,now);
    const old=this.decisions.get(key);if(old){old.n++;old.detailLast=detail;return;}this.decisions.set(key,{...c,n:1,atMs:now,detailFirst:detail,detailLast:detail});
  }
  onDecision(d){
    this.stats.sourceDecisions++;
    const timing=this.guard.status(this.session.engine.now);
    if(timing.status!=='ALLOW_SHADOW')this.stats.timingSuppressed++;
    this.addDecision({strategy:d.strategy,side:d.side??'?',signal:d.sig,baselineOk:d.ok,baselineReason:'SOURCE_PREFLIGHT_DECISION:'+d.why,timingStatus:timing.status,timingReasons:timing.reason_codes,
      stage:'BASELINE_PRE_QUEUE',entryReason:this.session.engine.entryPolicyReason({margin:2100,asset:'BTC'}),availableRaw:this.session.engine.available()});
  }
  observeCandidates(now){
    for(const c of this.session.bridge.api.observeCandidates()){
      const key=c.strategy+':'+c.side,second=Math.floor(now/1000);if(this.contextSeen.get(key)===second)continue;this.contextSeen.set(key,second);
      this.stats.rawCandidates++;const timing=this.guard.status(now),e=this.session.engine;
      const d=c.sourceDecision?.t===now?c.sourceDecision:null;
      let reason=e.entryPolicyReason({margin:2100,asset:'BTC'});
      if(!c.sourceEnabled||!c.sourceLiveBranch)reason=reason??'SOURCE_STOP_LATCHED';
      if(e.available()<2100n*10n**18n)reason=reason??'VIRTUAL_CAPITAL_INSUFFICIENT_FOR_FIXED_PRESET';
      this.addDecision({strategy:c.strategy,side:c.side,signal:c.signal,baselineOk:d?.ok??null,baselineReason:d?'FINAL_SOURCE_DECISION:'+d.why:'RAW_TRIGGER_NOT_FULL_BASELINE_PERMISSION',timingStatus:timing.status,timingReasons:timing.reason_codes,entryReason:reason,availableRaw:e.available(),stage:d?'AFTER_NATIVE_QUEUE_FILTERS':'RAW_TRIGGER_ONLY'});
      this.raw.trigger('candidate:'+key+':'+(reason??timing.status),now);
    }
  }
  capture(){
    const s=this.session;
    return {schemaVersion:1,modelHash:this.profile.modelHash,configHash:this.profile.configHash,vmStateProven:this.vmExact,observerStatus:this.status,
      session:{startMs:s.startMs,now:s.now,nextTick:s.nextTick,engine:captureEngine(s.engine),bridge:s.bridge.checkpointState(),inputCounts:s.inputCounts,lastInput:s.lastInput,lastInvalidate:s.lastInvalidate,receiveLatency:s.receiveLatency,eth:{counts:s.eth.counts,venues:s.eth.venues},global:s.router.global},
      continuity:this.continuity,guard:captureGuard(this.guard),guardEpoch:this.guardEpoch,stats:this.stats,lastObservedAtMs:this.lastObservedAtMs,connection:this.connection,recovery:this.recovery,storage:this.storage,raw:this.raw?.checkpoint()??null};
  }
  restore(state){
    if(state.schemaVersion!==1||state.modelHash!==this.profile.modelHash||state.configHash!==this.profile.configHash)throw new Error('PROFILE_CHECKPOINT_MISMATCH');
    const x=C.clone(state),s=this.session,v=x.session;
    restoreEngine(s.engine,v.engine);s.bridge.restoreState(v.bridge);s.bridge.api.bindPending(s.engine.intents);
    for(const k of ['startMs','now','nextTick','inputCounts','lastInput','lastInvalidate','receiveLatency'])s[k]=v[k];
    Object.assign(s.eth,v.eth);s.router.global=v.global;Object.assign(this.continuity,x.continuity);
    this.guardEpoch=x.guardEpoch;this.guard=this.makeGuard();restoreGuard(this.guard,x.guard);
    this.stats=x.stats;this.lastObservedAtMs=x.lastObservedAtMs;this.connection=x.connection;
    if(x.recovery)this.recovery=x.recovery;
    if(x.storage)this.storage=x.storage;
  }
  saveCheckpoint(clean,now){
    if(this.storage.dbBlocked)return false;
    return this.safeWrite(()=>{this.session.bridge.compactNative(now);this.checkpointInfo=this.history.checkpoint(this.capture(),{now,clean});});
  }
  proveResume(response,now){
    const r=this.recoveryRecord,e=this.session.engine;
    const contiguous=response.bootId===this.continuity.bootId&&!response.gap&&response.firstSequence<=this.continuity.cursor+1&&response.lastSequence>=this.continuity.cursor&&response.lastSequence-this.continuity.cursor<=response.events.length;
    const noDue=e.timers.every(t=>t.at>now),short=now-r.savedAtMs>=0&&now-r.savedAtMs<=1000&&now-this.session.now<=1000;
    const noUnseenPositionMarket=!e.positions.size||!response.events.some(x=>x.kind==='market'&&x.sequence>this.continuity.cursor);
    const exact=r.clean&&this.vmExact&&this.modelCompatible&&!r.corrupt?.length&&short&&contiguous&&noDue&&noUnseenPositionMarket;
    this.requiresHandshake=false;
    if(exact){this.recovery={...this.recovery,mode:'EXACT_SHORT_RESTART',censored:!!e.accountingQuarantined&&hasRisk(e),restoredWarmup:true};return;}
    const reason=this.recovery.reason??(!contiguous?'RESUME_BOOT_OR_GAP':!noDue?'DOWNTIME_EXECUTION_UNOBSERVED':!r.clean?'UNCLEAN_RESTART':'RESTART_PROOF_INSUFFICIENT');
    const risk=hasRisk(e);
    if(risk||!this.vmExact||!this.modelCompatible||r.corrupt?.length||!r.clean){censor(e,reason);this.recovery.requiresExplicitRecovery=true;}
    this.session.invalidate(reason,now);
    this.recovery={...this.recovery,mode:'QUARANTINE_WARMUP',reason,censored:risk,restoredWarmup:false};
    // No downtime timers execute at today's quote; retain them as censored evidence.
    if(risk)censor(e,reason);
    if(!risk&&this.vmExact&&this.modelCompatible&&!this.recovery.requiresExplicitRecovery)e.accountingQuarantined=false;
  }
  batch(response,now=this.clock()){
    validateEnvelope(response,256);
    if(this.requiresHandshake)this.proveResume(response,now);
    const previousBoot=this.continuity.bootId,b=this.continuity.consume(response);
    this.lastObservedAtMs=response.observedAtMs;this.connection=response.connection??null;this.stats.batches++;
    if(b.invalidated){
      const reason=b.notices[0]?.kind??'FEED_GAP',risk=hasRisk(this.session.engine);
      if(risk){censor(this.session.engine,reason);this.recovery={mode:'QUARANTINE_WARMUP',reason,censored:true,requiresExplicitRecovery:true};}
      this.session.invalidate(reason,now);this.suite?.invalidate(now,reason);
      this.safeWrite(()=>{for(const n of b.notices)this.history.quality(n.kind,n,now);});
      if(!risk){this.guardEpoch++;this.guard=this.makeGuard();this.guard.bootstrapAtCurrent(response,now,this.R3.NEW_POSITIONLESS_OBSERVATION);}
      else {this.guard.ingest(response,now);}
    }else this.guard.ingest(response,now);
    // Stage execution quotes before ANY model can fire a timer. With pagination,
    // do not run executions past the contiguous receive watermark still unread.
    const watermark=this.continuity.cursor>=response.lastSequence?response.observedAtMs:(b.events.at(-1)?.recv_ms??this.session.engine.now);
    const prices=this.session.engine.executionPrices;if(prices){if(b.invalidated)prices.reset(watermark);prices.stage(b.events,watermark);}
    this.suite?.stageExecution(b.events,watermark,{reset:b.invalidated});
    const clockOk=Math.abs(now-response.observedAtMs)<=this.profile.config.realtime.maxServerClockSkewMs;
    if(!clockOk){this.session.invalidate('SERVER_CLOCK_SKEW',now);this.suite?.invalidate(now,'SERVER_CLOCK_SKEW');if(hasRisk(this.session.engine)){censor(this.session.engine,'SERVER_CLOCK_SKEW');this.recovery.requiresExplicitRecovery=true;}}
    this.session.setContinuity(b.continuous&&clockOk);
    for(const event of b.events){
      this.stats.events++;this.stats[event.kind==='trade'?'publicTrades':event.kind]++;
      try{this.raw.observe(event,now,response.bootId);}catch(e){this.rawWarning(e.message);}
      this.session.ingest(event,now);this.suite?.ingest(event,now,b.continuous&&clockOk);
      if(event.kind==='market')this.aggregate(event);
      else if(this.evidence.length<512)this.evidence.push({event,boot:response.bootId});else this.rawWarning('PUBLIC_EVIDENCE_BUFFER_BOUND');
    }
    this.status='RUNNING';return {events:b.events.length,invalidated:b.invalidated,previousBoot};
  }
  aggregate(e){
    const second=Math.floor(e.recv_ms/1000)*1000,key=second+'|'+e.symbol+'|'+e.venue,p=e.payload,bid=Number(p.bid),ask=Number(p.ask);
    let s=this.market.get(key);if(!s){if(this.market.size>=128){this.rawWarning('MARKET_AGGREGATE_BUFFER_BOUND');return;}s={second,symbol:e.symbol,venue:e.venue,quoteN:0,bookN:0,bidMin:bid,bidMax:bid,askMin:ask,askMax:ask};this.market.set(key,s);}
    s[p.frameKind==='book'?'bookN':'quoteN']++;s.bidMin=Math.min(s.bidMin,bid);s.bidMax=Math.max(s.bidMax,bid);s.askMin=Math.min(s.askMin,ask);s.askMax=Math.max(s.askMax,ask);s.lastBid=p.bid;s.lastAsk=p.ask;s.recvMs=e.recv_ms;
  }
  observeSourceEvents(now){
    for(const event of this.session.bridge.api.events){
      if(event.observerSourceSequence)continue;
      let sequence=null;
      const committed=this.safeWrite(()=>{const previous=Number(this.history.getMeta('source_event_sequence')??0);if(!Number.isSafeInteger(previous)||previous<0)throw new Error('SOURCE_EVENT_SEQUENCE_CORRUPT');
        sequence=previous+1;this.history.setMeta('source_event_sequence',sequence);this.history.quality('SOURCE_MODEL_EVENT',{sourceSequence:sequence,sourceKind:event.kind,sourceAtMs:event.t,text:event.text,financialMode:'SIM_NOT_LIVE'},now);});
      event.observerSourceSequence=committed?sequence:'UNPERSISTED_STORAGE_LOSS';
      if(committed)this.stats.sourceModelEvents=sequence;
    }
  }
  heartbeat(now=this.clock()){
    if(this.requiresHandshake)return;
    this.session.advanceTo(now);this.observeSourceEvents(now);this.observeCandidates(now);this.suite?.advance(now);
    if(now-this.lastFlush>=1000)this.flush(now);
    if(now-this.lastCheckpoint>=5000){this.saveCheckpoint(false,now);this.lastCheckpoint=now;}
    if(now-this.lastRollup>=3600000){this.safeWrite(()=>this.history.rollup(now));this.lastRollup=now;}
  }
  flush(now){
    const ok=this.safeWrite(()=>{
      for(const s of this.market.values())this.history.market(s);
      for(const c of this.decisions.values()){for(let i=0;i<c.n;i++)this.history.decision(c,c.atMs);this.history.quality('DECISION_DETAILS',storedDecisionDetails(c,this.runId),c.atMs);}
      for(const p of this.forecasts)this.history.forecast(p);
      for(const x of this.evidence)this.history.evidence(x.event,x.boot);
    });
    if(ok){this.market.clear();this.decisions.clear();this.forecasts.length=0;this.evidence.length=0;this.raw.flush(now);this.cachedTotals=this.history.totals();boundRuntime(this.session.engine);}
    else {this.storage.historyLoss+=this.market.size+this.decisions.size+this.forecasts.length+this.evidence.length;this.market.clear();this.decisions.clear();this.forecasts=[];this.evidence=[];this.session.engine.ledger.length=0;}
    this.lastFlush=now;
  }
  explicitRecovery(ack,now=this.clock()){
    if(ack!=='RESEARCH_INPUT_RECOVERY_NO_RESET')throw new Error('EXPLICIT_RESEARCH_RECOVERY_ACK_REQUIRED');
    if(hasRisk(this.session.engine)||!canFreezeUnresolvedModels(this.history.db))throw new Error('UNRESOLVED_RISK_REQUIRES_ADJUDICATION_NO_RESET');
    if(!this.vmExact||!this.modelCompatible)throw new Error('VM_OR_MODEL_REVIEW_REQUIRED');
    this.history.checkSpace();if(this.raw.stats.orphanFiles)throw new Error('RAW_ORPHAN_REVIEW_REQUIRED');
    this.storage.dbBlocked=false;this.storage.healthy=true;this.storage.warnings=[];this.raw.disabled=false;this.raw.warning=null;
    this.session.engine.entryBlockReasons=[];this.session.engine.accountingQuarantined=false;this.session.invalidate('EXPLICIT_RESEARCH_RECOVERY',now);
    this.recovery={mode:'QUARANTINE_WARMUP',reason:'EXPLICIT_RESEARCH_RECOVERY',requiresExplicitRecovery:false,censored:false};
    this.guardEpoch++;this.guard=this.makeGuard();this.continuity=new Continuity();
    this.safeWrite(()=>this.history.quality('EXPLICIT_RESEARCH_RECOVERY_NO_CASH_RESET',{cashRaw:this.session.engine.balance.toString()},now));
    return {recovered:true,cashRaw:this.session.engine.balance.toString(),sourceStopStillLatched:!this.session.bridge.api.cfg.live};
  }
  snapshot(now=this.clock()){
    const data=this.session.snapshot(now),e=this.session.engine,guard=this.guard.status(now);
    const elapsed=now>=Math.max(this.session.startMs+180000,e.integrityWarmUntil),freshDepth=data.assets.BTC.warmup.complete;
    return {schemaVersion:1,mode:'READ_ONLY_RESEARCH_NO_ORDERS',financialMutations:0,realSignatures:0,generatedAtMs:now,runId:this.runId,status:this.status,
      input:{bootId:this.continuity.bootId,lastSequence:this.continuity.cursor,lastEvent:this.session.lastInput,lastObservedAtMs:this.lastObservedAtMs,eventAgeMs:this.session.lastInput?now-this.session.lastInput.recv_ms:null,continuity:this.continuity.counts,connection:this.connection,sourceURL:ENDPOINT},
      ...data,acceptance:{warmupElapsed:elapsed,completeWarmup:freshDepth,queueModelIdentified:false,cashPayoutVerified:false,performanceVerdict:'INCONCLUSIVE',LIVEPermission:false},
      service:{persistent:true,pid:process.pid,uptimeMs:(this.stoppedAt??now)-this.startedAt,restarts:this.segmentInfo.previous?this.segmentInfo.previous.restarts+1:0,startedAtMs:this.startedAt,accountOriginMs:this.session.startMs,stoppedAtMs:this.stoppedAt??null,completed:false},
      runtime:{state:this.status,stats:this.stats,counterCoverage:'Persisted prefix plus current segment; any uncommitted recovery interval remains unknown',sourceStopLatched:!this.session.bridge.api.cfg.live||!this.session.bridge.api.cfg.enabled,checkpoint:this.checkpointInfo,recovery:this.recovery,modelHash:this.profile.modelHash,configHash:this.profile.configHash,sourceProfile:this.profile.id,failure:this.failure},
      fixedPreset:{asset:'BTC',marginUsdc:2100,leverage:600,notionalUsd:1260000,strictlyGreaterThanUsd:1000000,actualWalletRead:false},
      history:{database:'shadow-history.sqlite',financialMode:'SIM',totals:this.cachedTotals??{},durableWritesHealthy:!this.storage.dbBlocked,marketAggregation:'1s observed frame tops/counts, NOT exact tick replay'},
      storage:{...this.storage,raw:this.raw.status()},comparison:this.suite?.snapshot()??null,timingGuard:guard,financialPerformance:'SIM_NOT_OBSERVED_CASH_PAYOUT'};
  }
  stop(reason='PARENT_STOP',now=this.clock()){
    if(this.status==='STOPPED')return this.snapshot(now);
    this.suite?.stop(now);this.observeSourceEvents(now);this.flush(now);this.raw.flush(now);this.stoppedAt=now;this.status='STOPPED';
    this.saveCheckpoint(true,now);this.safeWrite(()=>this.history.finish('STOPPED',now,{reason,recovery:this.recovery}));
    const final=this.snapshot(now);this.session.close();this.history.close();return final;
  }
}
module.exports={PersistentObserver};
