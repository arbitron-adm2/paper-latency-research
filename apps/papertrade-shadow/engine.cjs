'use strict';
const {raw,decimal,price,settle,UNIT}=require('./money.cjs');
const {createBridge,sha}=require('./strategy.cjs');
const {virtualEntryLimit,entryAllowed}=require('./deadline.cjs');
const {ExecutionPrices}=require('./execution-prices.cjs');
const json=x=>JSON.stringify(x,(_,v)=>typeof v==='bigint'?v.toString():v);
class Random {
  constructor(seed){this.state=seed>>>0;}
  next(){this.state=(Math.imul(this.state,1664525)+1013904223)>>>0;return this.state/4294967296;}
}
// Explicit evidence reducer: terminal beats stale snapshots; absence proves nothing.
class Reconciler {
  constructor(){this.positions=new Map();this.closed=new Map();this.seen=new Set();this.ambiguities=[];}
  apply(e){
    if(this.seen.has(e.id))return;
    this.seen.add(e.id);
    if(e.kind==='close'){
      if(!e.positionId||!e.evidence)throw new Error('Close needs positionId and execution evidence');
      this.closed.set(String(e.positionId),e);this.positions.delete(String(e.positionId));
    }else if(e.kind==='open'){
      if(!e.positionId||!e.intentId){this.ambiguities.push(e);return;}
      if(!this.closed.has(String(e.positionId)))this.positions.set(String(e.positionId),e);
    }else if(e.kind==='snapshot'){
      // Snapshots can add observations, never terminalise unseen positions.
      for(const p of e.positions||[])this.apply({...p,id:e.id+':'+p.positionId,kind:'open'});
    }else if(e.kind==='partial')this.ambiguities.push({...e,reason:'Partial execution accounting unsupported; risk retained'});
  }
}
class Execution {
  constructor(config,experiment,scenario,start){
    this.priceScale=BigInt(config.priceScale??10);this.config=config;this.experiment=experiment;this.scenario=scenario;this.now=start;
    this.rng=new Random(config.seed);this.intents=new Map();this.positions=new Map();this.timers=[];
    this.ledger=[];this.ledgerSequence=0;this.orders=[];this.seq=0;this.balance=raw(experiment.balance);this.reserved=0n;this.locked=0n;
    this.realised=0n;this.fees=0n;this.funding=0n;this.missingDepthTicks=0;this.samples={entry:[],exit:[],cancel:[]};
    this.counts={submitted:0,filled:0,cancelRequested:0,cancelled:0,cancelConflict:0,rejected:0,closed:0,lateFill:0,liquidated:0,entryExpired:0,localSignalExpired:0};
    this.bestEquity=this.balance;this.drawdown=0n;this.exposureMs=0;this.capitalMs=0n;
    if(config.executionPricePolicy){this.executionPrices=new ExecutionPrices(config.simProfile?.asset??'BTC');this.startPriceEpoch(config.executionPricePolicy.id,start);}
  }
  available(){return this.balance-this.reserved-this.locked;}
  startPriceEpoch(modelHash,at){this.priceEpoch={modelHash,startedAtMs:at,netRaw:0n,counts:{submitted:0,filled:0,closed:0,wins:0,losses:0,zeros:0,executionPriceUnknown:0}};}
  priceEpochSummary(){return this.priceEpoch?{...this.priceEpoch,netRaw:undefined,netUsdc:decimal(this.priceEpoch.netRaw),policy:this.config.executionPricePolicy.id}:null;}
  note(kind,data){
    const epoch=this.priceEpoch;
    if(epoch){
      if(kind==='submitted')epoch.counts.submitted++;
      if(kind==='entry_fill'&&data.priceEvidence&&data.executedAtMs>=epoch.startedAtMs)epoch.counts.filled++;
      if(kind==='execution_price_unavailable')epoch.counts.executionPriceUnknown++;
      if(kind==='close_fill'&&data.entryPriceEvidence&&data.exitPriceEvidence&&data.openedAt>=epoch.startedAtMs){const cash=BigInt(data.cash);epoch.netRaw+=cash;epoch.counts.closed++;epoch.counts[cash>0n?'wins':cash<0n?'losses':'zeros']++;}
    }
    const row={sequence:this.ledgerSequence++,time_ms:this.now,kind,...data};this.ledger.push(row);this.lastNoteCommitted=this.noteSink?this.noteSink(row)!==false:true;
  }
  recordDecision(strategy,side,sig,ok,decision){
    this.tickStats??={ticks:0,reasons:{},rawSignalTicks:0,signalTransitions:0,candidateEvaluations:0,approved:0,guardReasons:{}};
    this.tickStats.candidateEvaluations++;if(ok)this.tickStats.approved++;
    this.lastCandidate={time_ms:this.now,strategy,side,sig:Number.isFinite(sig)?sig:null,ok,why:decision?.why??null};
    if(this.decisionSink)this.decisionSink(this.lastCandidate);
  }
  schedule(at,kind,id){this.timers.push({at,kind,id,sequence:this.seq++});this.timers.sort((a,b)=>a.at-b.at||a.sequence-b.sequence);}
  delay(kind,notional){
    const s=this.scenario;
    if(s.ideal)return 0;
    const pool=s[kind+'Ms'];
    if(!pool?.length)throw new Error('Explicit scenario latency pool required');
    // Same congestion state for entry/exit/cancel within the time bucket, with past-only volatility.
    const phase=(Math.floor(this.now/10000)%7)/6;
    const depth=this.bridge?.api.feed;
    const mid=depth?(depth.hl.bid+depth.hl.ask)/2:NaN;
    const move=Number.isFinite(mid)&&Number.isFinite(this.lastPrice)?Math.min(2,Math.abs(mid-this.lastPrice)/10):0;
    const volume=s.volumeSlope?Math.max(0,Number(notional)/1260000)*s.volumeSlope:0;
    const factor=1+(s.congestion||0)*phase+(s.volatility||0)*move+volume;
    return Math.round(pool[Math.floor(this.rng.next()*pool.length)]*factor);
  }
  exitDelay(p){
    const b=this.scenario.exitBuckets;
    if(!Array.isArray(b)||!b.length)return this.delay('exit',this.experiment.margin*this.experiment.leverage);
    const hold=Math.max(0,this.now-Number(p.openedAt??this.now)),row=b.find(x=>x.maxHoldMs==null||hold<x.maxHoldMs)??b[b.length-1];
    const pool=Array.isArray(row.values)&&row.values.length?row.values:[row.medianMs];
    return Math.round(Number(pool[Math.floor(this.rng.next()*pool.length)]));
  }
  fallbackQuote(){
    if(this.executionPrices)return null;
    // Vlad paper mode fills at the last known HL mid even when our own input is marked unhealthy.
    // Bounded by age; never used while accounting is quarantined.
    const max=this.scenario.fillFallbackMaxAgeMs;if(!(max>0)||this.accountingQuarantined)return null;
    const f=this.bridge?.api.feed?.hl;if(!f||!(f.bid>0&&f.ask>=f.bid)||this.now-f.t>max)return null;
    if(f.ask-f.bid>(this.bridge.maxFillSpread??6))return null;
    return BigInt(Math.round((f.bid+f.ask)/2*Number(this.priceScale)));
  }
  entryPolicyReason(q){
    if(!this.config.persistentService?.enabled)return null;
    const nominal=raw(q.margin)*BigInt(this.experiment.leverage);
    if(nominal<=1000000n*UNIT)return 'NOTIONAL_NOT_STRICTLY_GREATER_THAN_1M';
    if(raw(q.margin)!==2100n*UNIT||this.experiment.leverage!==600||nominal!==1260000n*UNIT||(q.asset&&q.asset!==(this.config.simProfile?.asset??'BTC')))return 'FIXED_BTC_2100_X_600_PRESET_REQUIRED';
    if(this.accountingQuarantined)return 'ACCOUNTING_RECOVERY_QUARANTINE';
    if(this.entryBlockReasons?.length)return this.entryBlockReasons.join('|');
    const gate=this.entryGate?.(q);return gate?.allowed===false?(gate.reason||'R3_TIMING_SUPPRESSED'):null;
  }
  submit(q){
    const policy=this.entryPolicyReason(q);
    if(policy){this.counts.entryPolicyRejected=(this.counts.entryPolicyRejected??0)+1;this.lastEntryPolicyReason=policy;
      // Refusal changes no financial exposure. Decisions retain every observation;
      // identical refusal notes need at most one durable write per second.
      this.policyNoteLast??=new Map();const key=[q.strategy,q.side,policy].join('|'),second=Math.floor(this.now/1000);
      if(this.policyNoteLast.get(key)!==second){this.policyNoteLast.set(key,second);
        if(this.policyNoteLast.size>128)for(const [k,t]of this.policyNoteLast)if(t<second-1)this.policyNoteLast.delete(k);
        this.note('entry_not_submitted',{reason:policy,strategy:q.strategy,side:q.side,notional_usd:Number(raw(q.margin)*BigInt(this.experiment.leverage))/1e18,financialPerformance:'simulated',intentCreated:false,grouping:'one_per_second_strategy_side_reason; all_attempts_in_counters_and_decisions'});
      }return null;}
    const id='shadow-entry-'+(++this.seq),m=raw(q.margin),n=m*BigInt(this.experiment.leverage);
    const record={id,kind:'entry',createdAt:this.now,margin:m,notional:n,q,state:'submitted'};
    const ttl=this.config.entryDeadline?.signalTtlMs??15000;
    const validUntil=Math.min(q.ctx?.signalValidUntilMs??this.now+ttl,this.now+ttl,this.config.researchWindow?.endMs??Infinity);
    Object.defineProperty(record,'entryLimit',{value:virtualEntryLimit({intentId:id,decidedAtMs:Math.floor(this.now),signalValidUntilMs:Math.floor(validUntil),safetySeconds:this.config.entryDeadline?.safetySeconds??1}),enumerable:true});
    this.intents.set(id,record);
    this.note('virtual_entry_limit',{intentId:id,limit:record.entryLimit,financialSignatureCreated:false});
    if(this.config.persistentService?.enabled&&this.lastNoteCommitted===false){this.intents.delete(id);this.counts.entryPolicyRejected=(this.counts.entryPolicyRejected??0)+1;this.lastEntryPolicyReason='DURABLE_PREQUEUE_WRITE_FAILED_NO_ENQUEUE';return null;}
    if(this.config.persistentService?.enabled&&(this.accountingQuarantined||this.entryBlockReasons?.length)){
      if(this.accountingQuarantined){const current=this.intents.get(id)??record;current.state='outcome_unknown';if(this.reserved<m)this.reserved+=m;this.note('prequeue_censored',{intentId:id,reason:'STORAGE_OR_RECOVERY_INTERRUPTED',risk:'retained'});}
      else {record.state='rejected';this.counts.entryPolicyRejected=(this.counts.entryPolicyRejected??0)+1;this.note('entry_not_queued',{intentId:id,reason:this.entryBlockReasons.join('|'),localOnly:true});this.schedule(this.now,'reject',id);}
      return id;
    }
    this.counts.submitted++;this.note('submitted',{intentId:id,margin_raw:m.toString(),notional_raw:n.toString(),rank:null,rank_status:'unidentifiable counterfactual'});
    // Admission is a scenario, not a reconstruction of the hidden server/house state.
    const minimum=raw(this.config.limits.minimumNotional),minMargin=raw(this.config.limits.minimumMargin);
    if(this.available()<m||m<minMargin||n<minimum||this.experiment.leverage>this.config.limits.maximumLeverage){
      record.state='rejected';this.counts.rejected++;this.schedule(this.now,'reject',id);return id;
    }
    this.reserved+=m;record.state='queued';record.entryDelay=this.delay('entry',Number(n)/1e18);
    this.schedule(this.now+record.entryDelay,'entry',id);
    this.note('queued',{intentId:id,delay_ms:record.entryDelay,model:this.scenario.id,admission:'hypothetical capacity only; server OI unknown'});
    if(this.scenario.lostAck)this.note('ack_unknown',{intentId:id,risk_slot:'retained; no new intent'});
    return id;
  }
  cancel(id,reason){
    const r=this.intents.get(id);if(!r||['cancelled','rejected','executed'].includes(r.state))return;
    if(r.cancelRequested)return;
    r.cancelRequested=this.now;r.cancelReason=reason;this.counts.cancelRequested++;
    this.note('cancel_requested',{intentId:id,reason});
    this.schedule(this.now+this.delay('cancel',Number(r.notional)/1e18),'cancel',id);
  }
  close(p){
    const id='shadow-close-'+p.positionId;
    if(this.intents.has(id))return id;
    const record={id,kind:'exit',createdAt:this.now,positionId:String(p.positionId),state:'queued',reason:p.closing.why};
    this.intents.set(id,record);
    const delay=this.exitDelay(p);
    this.note('close_requested',{intentId:id,positionId:record.positionId,delay_ms:delay,reason:record.reason});
    this.schedule(this.now+delay,'exit',id);
    if(this.scenario.lostAck)this.note('close_ack_unknown',{intentId:id,retry_identity:id});
    return id;
  }
  quote(){
    if(this.executionPrices){
      if(this.accountingQuarantined)return null;
      const q=this.executionQuote(this.now);return q?BigInt(Math.round((q.bid+q.ask)/2*Number(this.priceScale))):null;
    }
    if(this.inputHealthy===false||this.accountingQuarantined)return null;
    const f=this.bridge.api.feed.hl;
    if(!(f.bid>0&&f.ask>=f.bid)||this.now-f.t>this.config.fillMaxAgeMs)return null;
    if(f.ask-f.bid>(this.bridge.maxFillSpread??6))return null;
    // Fill-price is only a scenario proxy, not the displayed quote or future server fill.
    return BigInt(Math.round((f.bid+f.ask)/2*Number(this.priceScale)));
  }
  executionQuote(at){return this.executionPrices?.at(at,{maxAgeMs:this.config.executionPricePolicy.maxAgeMs,maxSpread:this.bridge?.maxFillSpread??6})??null;}
  fire(t){
    const r=this.intents.get(t.id);if(!r)return;
    if(t.kind==='reject'){this.bridge.api.terminal(r.q);this.note('rejected',{intentId:r.id});return;}
    if(this.accountingQuarantined){if(['submitted','queued'].includes(r.state))r.state='outcome_unknown';this.note('execution_censored',{intentId:r.id,reason:'ACCOUNTING_RECOVERY_QUARANTINE',risk:'retained'});return;}
    if(t.kind==='cancel'){
      this.samples.cancel.push(this.now-r.cancelRequested);
      if(r.state!=='queued')return;
      const lead=this.scenario.cancelMinLeadMs,entryTimer=lead!=null?this.timers.find(x=>x.id===r.id&&x.kind==='entry'):null;
      let conflict=lead!=null?!(entryTimer&&entryTimer.at-r.cancelRequested>=lead):this.rng.next()<this.scenario.cancelConflictProbability;
      // live preset: enough lead is necessary but not sufficient; measured live success given lead>=1500 ms is ~13%.
      if(!conflict&&lead!=null&&this.scenario.cancelSuccessProbability!=null)conflict=this.rng.next()>=this.scenario.cancelSuccessProbability;
      if(conflict){r.q.cancelFailed='intent_conflict';this.counts.cancelConflict++;this.note('cancel_conflict',{intentId:r.id,interpretation:'scenario only; outcome not proven'});return;}
      if(this.scenario.cancelAckOnly){this.note('cancel_accepted_unconfirmed',{intentId:r.id,risk_slot:'retained'});return;}
      r.state='cancelled';this.reserved-=r.margin;this.counts.cancelled++;
      this.bridge.api.terminal(r.q);this.note('cancel_terminal',{intentId:r.id});return;
    }
    if((t.kind==='entry'&&r.state!=='queued')||(t.kind==='exit'&&r.state!=='queued'))return;
    if(t.kind==='entry'&&!entryAllowed(r.entryLimit,Math.floor((this.executionPrices?t.at:this.now)/1000))){this.expireEntry(r,Math.floor((this.executionPrices?t.at:this.now)/1000));return;}
    if(this.scenario.censored){r.state='outcome_unknown';this.note('censored',{intentId:r.id,risk_slot:'retained'});return;}
    const executionAt=this.executionPrices?t.at:this.now,priceEvidence=this.executionPrices?this.executionQuote(executionAt):null;
    let px=this.executionPrices?(priceEvidence?BigInt(Math.round((priceEvidence.bid+priceEvidence.ask)/2*Number(this.priceScale))):null):this.quote();
    if(this.executionPrices&&px===null&&!this.scenario.nonstop){
      r.state='outcome_unknown';r.executionPriceMissing=true;r.executionAtMs=executionAt;
      this.counts.executionPriceUnknown=(this.counts.executionPriceUnknown??0)+1;
      this.note('execution_price_unavailable',{intentId:r.id,intentKind:t.kind,executionAtMs:executionAt,observedAtMs:this.now,policy:this.config.executionPricePolicy.id,risk:'reserved/locked capital retained; no cancellation, no cash PnL, no fill at a later price'});
      return;
    }
    if(px===null&&this.scenario.fillFallbackMaxAgeMs>0){px=this.fallbackQuote();if(px!==null)this.note('fill_price_proxy',{intentId:r.id,intentKind:t.kind,proxy:'last HL mid within fallback age; input unhealthy or quote older than fillMaxAgeMs',maxAgeMs:this.scenario.fillFallbackMaxAgeMs});}
    if(px===null&&(t.kind==='exit'||t.kind==='entry')&&this.scenario.exitPriceRetryMs>0&&this.now-(r.firstPriceMissAt??=this.now)<this.scenario.exitPriceRetryMs){
      r.priceRetries=(r.priceRetries??0)+1;if(r.priceRetries===1)this.note('price_retry',{intentId:r.id,intentKind:t.kind,retryMs:250,untilMs:r.firstPriceMissAt+this.scenario.exitPriceRetryMs});
      this.schedule(this.now+250,t.kind,r.id);return;
    }
    if(px===null&&this.scenario.nonstop){
      // Non-stop SIM: a close waits for the first fresh HL mid (never outcome_unknown); an entry without price is released.
      // Causal execution prices: each retry reads only the quote at its own later time; nothing is backfilled.
      if(this.executionPrices&&(t.kind==='entry'||r.firstPriceMissAt==null)){
        this.counts.executionPriceUnknown=(this.counts.executionPriceUnknown??0)+1;
        this.note('execution_price_unavailable',{intentId:r.id,intentKind:t.kind,executionAtMs:executionAt,observedAtMs:this.now,policy:this.config.executionPricePolicy.id,
          risk:t.kind==='exit'?'nonstop SIM: locked capital retained while the exit waits for the first fresh causal HL quote; wait recorded':'nonstop SIM: entry not filled; reserved margin released, no position, no PnL'});
      }
      if(t.kind==='exit'){
        r.firstPriceMissAt??=this.now;r.priceWaits=(r.priceWaits??0)+1;
        if(r.priceWaits===1||r.priceWaits%120===0)this.note('exit_price_wait',{intentId:r.id,waitingSinceMs:r.firstPriceMissAt,retryMs:500,policy:'fill at first fresh HL mid; delay recorded'});
        this.schedule(this.now+500,'exit',r.id);return;
      }
      r.state='expired';this.reserved-=r.margin;this.counts.entryExpired++;
      this.bridge.api.terminal(r.q);this.note('entry_price_unavailable_released',{intentId:r.id,executionAtMs:executionAt,policy:'no fresh HL mid at fill time; margin released, no position'});return;
    }
    if(px===null){
      // Unknown executable price cannot be backfilled from the future. Keep risk and mark censored.
      r.state='outcome_unknown';this.note('fill_price_unavailable',{intentId:r.id,risk_slot:'retained'});return;
    }
    if(t.kind==='entry'){
      this.observeEntry(r.id,{executedAtMs:executionAt,observedAtMs:this.now,executionBlockSeconds:Math.floor(executionAt/1000),priceRaw:px,priceEvidence});
    }else{
      const p=this.positions.get(r.positionId);
      if(!p){r.state='executed';return;}
      const priceWait=this.scenario.nonstop&&r.firstPriceMissAt!=null?{firstMissAtMs:r.firstPriceMissAt,waitMs:executionAt-r.firstPriceMissAt,retries:r.priceWaits??0}:null;
      this.finish(p,px,r.reason,false,{executionAtMs:executionAt,priceEvidence,priceWait});r.state='executed';this.samples.exit.push(executionAt-r.createdAt);
      if(priceWait)this.note('exit_filled_after_price_wait',{intentId:r.id,waitMs:this.now-r.firstPriceMissAt,executionAtMs:executionAt});
    }
  }
  expireEntry(r,blockSeconds){
    if(!['queued','outcome_unknown'].includes(r.state))return;
    r.state='expired';this.reserved-=r.margin;this.counts.entryExpired++;
    this.timers=this.timers.filter(t=>!(t.id===r.id&&t.kind==='entry'));
    this.bridge.api.terminal(r.q);this.note('entry_limit_rejected',{intentId:r.id,executionBlockSeconds:blockSeconds,deadlineSec:r.entryLimit.deadlineSec,
      evidence:'virtual execution-limit check, not cancellation acknowledgement',financialPerformance:'simulated'});
  }
  observeEntry(id,{executedAtMs,observedAtMs=this.now,executionBlockSeconds,priceRaw,priceEvidence=null}){
    if(this.accountingQuarantined){this.note('execution_observation_not_materialized',{intentId:id,reason:'RECOVERY_QUARANTINE'});return false;}
    const r=this.intents.get(id);
    if(!r||r.kind!=='entry'||!['queued','outcome_unknown'].includes(r.state))return false;
    if(observedAtMs!==this.now||executedAtMs>observedAtMs||executedAtMs<r.createdAt||Math.floor(executedAtMs/1000)!==executionBlockSeconds)throw new Error('Invalid causal execution observation');
    if(!entryAllowed(r.entryLimit,executionBlockSeconds)){this.expireEntry(r,executionBlockSeconds);return false;}
    const px=BigInt(priceRaw);if(px<=0n)throw new Error('Invalid execution price');
    r.state='executed';this.reserved-=r.margin;this.locked+=r.margin;this.counts.filled++;
    this.timers=this.timers.filter(t=>!(t.id===r.id&&t.kind==='entry'));
    this.samples.entry.push(executedAtMs-r.createdAt);
    const positionId='virtual-'+r.id;
    const localExpired=observedAtMs>r.entryLimit.signalValidUntilMs;
    const late=!!(r.cancelRequested||r.q.cancelReason||r.q.signalGone||localExpired);if(late)this.counts.lateFill++;
    if(localExpired&&!r.q.cancelReason)r.q.cancelReason='local signal expired before fill observation';
    this.positions.set(positionId,{positionId,intentId:r.id,margin:r.margin,leverage:this.experiment.leverage,long:r.q.side==='L',entry:px,openedAt:executedAtMs,late,entryPriceEvidence:priceEvidence});
    this.note('entry_fill',{intentId:r.id,positionId,price_raw:px.toString(),executedAtMs,observedAtMs,observationLagMs:observedAtMs-executedAtMs,
      executionBlockSeconds,deadlineSec:r.entryLimit.deadlineSec,late,priceEvidence,price_model:'hypothetical causal HL midpoint / explicit synthetic observation',financialPerformance:'simulated'});
    this.bridge.api.fill(r.q,{positionId,intentId:r.id,entry:Number(px)/Number(this.priceScale),margin:Number(r.margin)/1e18,leverage:this.experiment.leverage,openedAt:executedAtMs});
    return true;
  }
  finish(p,px,reason,liquidated,{executionAtMs=this.now,priceEvidence=null,priceWait=null}={}){
    if(this.accountingQuarantined)return false;
    if(!this.positions.has(p.positionId))return;
    const payment=settle({...p,exit:px,liquidated},this.scenario);
    this.locked-=p.margin;this.balance+=payment.cash;this.realised+=payment.cash;this.fees+=payment.fee;this.funding+=payment.funding;
    this.counts.closed++;if(liquidated)this.counts.liquidated++;
    const record={positionId:p.positionId,intentId:p.intentId,openedAt:p.openedAt,closedAt:executionAtMs,observedAtMs:this.now,entryPriceEvidence:p.entryPriceEvidence??null,exitPriceEvidence:priceEvidence,
      entry_raw:p.entry.toString(),exit_raw:px.toString(),margin_raw:p.margin.toString(),leverage:p.leverage,
      long:p.long,late:p.late,reason,liquidated,...(priceWait?{exitPriceWait:priceWait}:{}),...Object.fromEntries(Object.entries(payment).map(([k,v])=>[k,typeof v==='bigint'?v.toString():v]))};
    this.orders.push(record);this.positions.delete(p.positionId);this.note('close_fill',record);
    const api=this.bridge.api,managed=[...api.positions.values()].find(x=>x.positionId===p.positionId);
    if(managed){api.closePx.set(p.positionId,{px:Number(px)/Number(this.priceScale),ts:this.now,source:'shadow-scenario',realPnl:Number(payment.cash)/1e18});
      api.closePosition(managed,reason,Number(px)/Number(this.priceScale),executionAtMs);
      this.bridge.db.prepare('UPDATE '+(this.bridge.tableName??'btcy2_lag_trades')+' SET real_pnl=?,est_pnl=? WHERE id=?').run(Number(payment.cash)/1e18,Number(payment.cash)/1e18,managed.id);
    }
  }
  mark(t){
    const elapsed=t-this.now;
    if(elapsed<0)throw new Error('Clock reversal');
    this.exposureMs+=elapsed*this.positions.size;this.capitalMs+=BigInt(Math.round(elapsed))*(this.locked+this.reserved);
    this.now=t;this.bridge.setTime(t);
    const px=this.quote();let unrealised=0n;
    if(px!==null){
      for(const p of [...this.positions.values()]){
        const payment=settle({...p,exit:px},this.scenario);unrealised+=payment.gross;
        // Explicit hypothetical threshold; exact contract rounding/house oracle remain unsupported.
        if(!this.scenario.ideal&&this.config.bustBufferRaw){
          const buffer=BigInt(this.config.bustBufferRaw),lev=BigInt(p.leverage);
          const threshold=p.long?p.entry*(lev-1n)*UNIT/(lev*(UNIT-buffer)):p.entry*(lev+1n)*UNIT/(lev*(UNIT+buffer));
          if(p.long?px<=threshold:px>=threshold)this.finish(p,px,'hypothetical liquidation',true,{priceEvidence:this.executionPrices?this.executionQuote(t):null});
        }
      }
      this.lastPrice=Number(px)/Number(this.priceScale);
      // Recompute after any liquidation; no unrealised double count on terminal positions.
      unrealised=0n;for(const p of this.positions.values())unrealised+=settle({...p,exit:px},this.scenario).gross;
      const equity=this.balance+unrealised;if(equity>this.bestEquity)this.bestEquity=equity;
      const dd=this.bestEquity-equity;if(dd>this.drawdown)this.drawdown=dd;
    }
  }
  summary(){return {mode:'simulate',scenario:this.scenario.id,experiment:this.experiment,status:'INCONCLUSIVE',
    model_class:this.scenario.ideal?'diagnostic frictionless benchmark, not a mathematical upper bound':'explicit hypothesis, not a calibrated queue probability',executionPricePolicy:this.config.executionPricePolicy??null,priceEpoch:this.priceEpochSummary(),
    measured_counterfactual_rank:null,counts:this.counts,net_pnl_scenario:decimal(this.realised),
    cash_payout_verified:false,available_scenario:decimal(this.available()),reserved_scenario:decimal(this.reserved),locked_scenario:decimal(this.locked),
    fees_scenario:decimal(this.fees),funding_scenario:decimal(this.funding),max_drawdown_gross_mark:decimal(this.drawdown),
    exposure_ms:this.exposureMs,capital_usdc_ms:decimal(this.capitalMs),latency_ms:this.samples,
    signal_and_gate_counts:this.tickStats??null,last_candidate:this.lastCandidate??null,entryDeadline:{enabled:true,contractEqualityAllowed:true,realSignatures:0,safetySeconds:this.config.entryDeadline?.safetySeconds??1},
    missing_depth_ticks:this.missingDepthTicks,unresolved_intents:[...this.intents.values()].filter(x=>['queued','outcome_unknown'].includes(x.state)).map(x=>({id:x.id,kind:x.kind,state:x.state})),
    open_positions:[...this.positions.values()].map(x=>({...x,margin:x.margin.toString(),entry:x.entry.toString()})),
    assetScope:{BTC:'reviewed baseline',ETH:'observe-only/UNCONFIGURED',observed:this.assetCoverage??{}},
    coverage:{notional_observations:0,out_of_support:true,calibration:'none; small contaminated historical labels are advisory only',independent_validation:false},
    unsupported:['server volume/rank algorithm','exact execution oracle','nonlinear payout and caps','exact funding and liquidation rounding','unobserved house impact'],
    financial_mutations_sent:0,network_requests:0,secrets_read:0};}
}
async function simulate(events,config,launch,experiment,scenario,{until=null}={}){
  if(!events.length)throw new Error('Empty tape: cannot simulate');
  events=[...events].sort((a,b)=>a.recv_ms-b.recv_ms||String(a.source).localeCompare(String(b.source))||a.sequence-b.sequence);
  const start=Math.floor(events[0].recv_ms/100)*100,end=until??Math.ceil(events.at(-1).recv_ms/100)*100;
  if(end<start||end-start>3600000)throw new Error('Bounded simulation window required');
  const engine=new Execution(config,experiment,scenario,start);
  const bridge=createBridge(engine,launch,experiment,start);engine.bridge=bridge;
  let index=0,tick=start;
  const seen=new Set();
  try{
    while(true){
      const next=Math.min(events[index]?.recv_ms??Infinity,engine.timers[0]?.at??Infinity,tick);
      if(next>end)break;
      engine.mark(next);
      while(index<events.length&&events[index].recv_ms===next){
        const e=events[index++];
        const digest=e.capture_record_sha256??e.payload_sha256??e.raw_sha256;
        if(e.schema!==1||!Number.isFinite(e.event_ms)||!Number.isFinite(e.recv_ms)||!digest||!['BTC','ETH'].includes(e.market))throw new Error('Invalid canonical event schema');
        const id=e.source+':'+e.sequence+':'+digest;
        if(!seen.has(id)){
          engine.assetCoverage??={BTC:0,ETH:0};engine.assetCoverage[e.market]++;
          if(e.market==='BTC')bridge.market(e);seen.add(id);
        }else engine.note('duplicate_ignored',{event:id});
      }
      while(engine.timers[0]?.at===next){engine.fire(engine.timers.shift());}
      if(tick===next){bridge.step(next);tick+=100;}
      // Promise callbacks from original async sendClose are local only.
      await Promise.resolve();
    }
    engine.mark(end);
    const summary=engine.summary();summary.input_events=events.length;
    summary.strategy_source=bridge.sourceManifest;summary.ledger_sha256=sha(json(engine.ledger));
    summary.config_sha256=sha(json({config,launch,experiment,scenario}));
    const sourceRows=bridge.db.prepare('SELECT * FROM btcy2_lag_trades ORDER BY id').all();
    return {summary,ledger:engine.ledger,positions:engine.orders,strategy_rows:sourceRows,
      checkpoint:{schema:1,type:'deterministic-prefix-replay',through_ms:end,config,launch,experiment,scenario,
        events:events.filter(e=>e.recv_ms<=end),source_manifest:bridge.sourceManifest,ledger_sha256:summary.ledger_sha256}};
  }finally{bridge.close();}
}
async function resume(checkpoint,additional=[]){
  const {sourceManifest}=require('./strategy.cjs');
  if(json(sourceManifest())!==json(checkpoint.source_manifest))throw new Error('Strategy source changed since checkpoint');
  const first=await simulate(checkpoint.events,checkpoint.config,checkpoint.launch,checkpoint.experiment,checkpoint.scenario,{until:checkpoint.through_ms});
  if(first.summary.ledger_sha256!==checkpoint.ledger_sha256)throw new Error('Checkpoint ledger diverged');
  return simulate([...checkpoint.events,...additional],checkpoint.config,checkpoint.launch,checkpoint.experiment,checkpoint.scenario);
}
module.exports={Execution,Random,Reconciler,simulate,resume,json};
