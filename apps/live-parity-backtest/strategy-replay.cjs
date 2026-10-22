'use strict';
// Offline, isolated research adapter. Never imports the production account/client/runtime.
const fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto');
const Module=require('node:module'),originalLoad=Module._load;
const denied=new Set(['net','http','https','http2','tls','dns','dgram','undici','ws','ethers']);
Module._load=function(id,...args){if(denied.has(id.replace(/^node:/,'')))throw new Error('OFFLINE_NETWORK_DENIED:'+id);return originalLoad.call(this,id,...args);};
globalThis.fetch=()=>{throw new Error('OFFLINE_NETWORK_DENIED');};
globalThis.WebSocket=class{constructor(){throw new Error('OFFLINE_NETWORK_DENIED');}};

function installObservation(engine,calibration){
  const api=engine.bridge.api,fill=api.fill,close=api.closePosition,fire=engine.fire.bind(engine);
  const pending=new Map();let serial=0;
  const sample=a=>a[Math.floor(engine.rng.next()*a.length)];
  api.fill=function(q,x){
    const id='parity-visible-entry-'+(++serial);
    pending.set(id,{kind:'entry',q,x});
    const lag=sample(calibration.entryObservationMs);
    engine.schedule(engine.now+lag,'parity_observation',id);
    engine.note('parity_entry_visibility_scheduled',{intentId:q.intentId,physicalAtMs:engine.now,visibleAtMs:engine.now+lag});
  };
  api.closePosition=function(...args){
    const id='parity-visible-close-'+(++serial),lag=sample(calibration.exitObservationMs);
    pending.set(id,{kind:'exit',args});engine.schedule(engine.now+lag,'parity_observation',id);
    engine.note('parity_close_visibility_scheduled',{positionId:args[0].positionId,physicalAtMs:engine.now,visibleAtMs:engine.now+lag});
  };
  engine.fire=function(t){
    if(t.kind!=='parity_observation')return fire(t);
    const r=pending.get(t.id);if(!r)return;pending.delete(t.id);
    if(r.kind==='entry'){
      const physical=engine.positions.get(r.x.positionId);
      if(physical&&!physical.late&&(r.q.signalGone||r.q.cancelReason)){
        physical.late=true;engine.counts.lateFill++;
      }
      fill.call(api,r.q,r.x);
      // A liquidation can happen before the bot learns of entry: retain physical accounting.
      if(!engine.positions.has(r.x.positionId)){
        const order=engine.orders.find(x=>x.positionId===r.x.positionId);
        const managed=[...api.positions.values()].find(x=>x.positionId===r.x.positionId);
        if(order&&managed){api.closePx.set(r.x.positionId,{px:Number(order.exit_raw)/Number(engine.priceScale),ts:order.closedAt,source:'parity-physical-close',realPnl:Number(order.cash)/1e18});close.call(api,managed,order.reason,Number(order.exit_raw)/Number(engine.priceScale),order.closedAt);}
      }
    }else close.apply(api,r.args);
    engine.note('parity_execution_visible',{intentKind:r.kind,atMs:engine.now});
  };
  return pending;
}

function installTrainQueue(e,cal,{entryTtlMs=20000}={}){
  const prior=e.delay.bind(e);
  e.delay=function(kind,notional){
    const pool=kind==='entry'?cal.entryBlocks:kind==='exit'?cal.exitBlocks:null;
    if(!pool)return prior(kind,notional);
    const blocks=pool[Math.floor(this.rng.next()*pool.length)];
    return Math.max(1,Math.floor(this.now/1000)*1000+blocks*1000-this.now);
  };
  e.exitDelay=function(){return this.delay('exit',this.experiment.margin*this.experiment.leverage);};
  e.scenario={...e.scenario,congestion:0,volatility:0,volumeSlope:0,cancelMinLeadMs:0,
    cancelSuccessProbability:cal.cancelSuccessProbability,fillFallbackMaxAgeMs:0,exitPriceRetryMs:1000};
  e.config.entryDeadline.signalTtlMs=entryTtlMs;
}

function sizingFor(plan,id){
  const s=plan.variantSettings?.[id]?.sizing??plan.sizing??{margin:20,leverage:100,initialVirtualCapital:2300};
  const capital=Number(s.initialVirtualCapital??2300),margin=Number(s.margin),leverage=Number(s.leverage);
  if(!Number.isFinite(margin)||margin<=0||!Number.isInteger(leverage)||leverage<1||leverage>1000||!Number.isFinite(capital)||capital<margin)throw new Error('INVALID_RESEARCH_SIZING');
  return{margin,leverage,notional:margin*leverage,initialVirtualCapital:capital};
}

function installExpiryReconciliation(e,graceMs=30000){
  const submit=e.submit.bind(e),fire=e.fire.bind(e);
  e.submit=function(q){
    const id=submit(q),r=this.intents.get(id);
    if(r?.state==='queued')this.schedule((r.entryLimit.deadlineSec+1)*1000+graceMs,'parity_signed_expiry',id);
    return id;
  };
  e.fire=function(t){
    if(t.kind!=='parity_signed_expiry')return fire(t);
    const r=this.intents.get(t.id);
    if(!r||!['queued','outcome_unknown'].includes(r.state))return;
    this.expireEntry(r,Math.floor(this.now/1000));
    this.note('parity_signed_expiry_reconciled',{intentId:r.id,graceMs,
      evidence:'Simulated fresh-wallet expiry after the production 30s grace; NOT historical provider evidence'});
  };
}

async function run(folder,seedOffset=0){
  const read=n=>JSON.parse(fs.readFileSync(path.join(folder,n),'utf8'));
  const plan=read('plan.json'),cal=read('calibration.json');
  if(cal.maxLabelTime>=plan.split)throw new Error('TRAIN_LABEL_LEAK');
  const source=path.join(folder,'source');
  const runnerSha256=crypto.createHash('sha256').update(fs.readFileSync(__filename)).digest('hex');
  for(const r of read('source-manifest.json')){
    if(crypto.createHash('sha256').update(fs.readFileSync(path.join(source,r.path))).digest('hex')!==r.sha256)throw new Error('FROZEN_SOURCE_DRIFT:'+r.path);
  }
  const {Replay}=require(path.join(source,'apps/strategy-lab/replay.cjs'));
  const profiles=read('strategy-profiles.json');
  const baseline=profiles.find(p=>p.id==='vlad_y2_p31_live_twin');
  if(!baseline)throw new Error('PROFILE_NOT_FOUND');
  const variants=plan.variants;
  const replay=new Replay({start:plan.start,end:plan.end,split:plan.split,warmupMs:plan.warmupMs,profiles:variants.map(id=>({...baseline,id})),capital:2300});
  const ms=s=>Date.parse('2026-10-20T'+s+'+03:00');
  // Known operator configuration/enable windows, NOT chosen from strategy PnL.
  const windows=[[ms('22:20:31'),ms('22:39:54')],[ms('22:42:31'),ms('23:03:00')],[ms('23:06:35'),ms('23:22:36')]];
  const switchAt=ms('22:41:00'),pendingById=new Map();
  for(const m of replay.models){
    const setting=plan.variantSettings?.[m.id]??{},sizing=sizingFor(plan,m.id);
    const modelCal=setting.calibration?read(setting.calibration):cal;
    if(modelCal.maxLabelTime>=plan.split)throw new Error('TRAIN_LABEL_LEAK:'+m.id);
    const e=m.engine;e.rng.state=(cal.seed+seedOffset)>>>0;
    if(sizing.initialVirtualCapital!==2300)throw new Error('THIS_ADAPTER_REQUIRES_FIXED_2300_ACCOUNT');
    Object.assign(e.experiment,{margin:sizing.margin,leverage:sizing.leverage});Object.assign(m.bridge.api.cfg,{margin:sizing.margin,leverage:sizing.leverage});
    // Without the recorded public environment, native source falls back to $10k and rejects every real $2k probe.
    // Fill only the known public minimum. Hidden OI/capacity remains unknown, not invented.
    m.bridge.api.oi.minNotional=Number(e.config.limits.minimumNotional);
    // Research-only DI removes the persistent comparator's 1.26M sizing requirement and virtual top-ups.
    // Physical execution/accounting and source strategy remain in the reviewed sandbox.
    m.entryGate=function(){
      if(!e.inputHealthy||e.now<e.integrityWarmUntil)return{allowed:false,reason:'INPUT_WARMUP_OR_GAP'};
      if(!windows.some(([a,b])=>a<=e.now&&e.now<b))return{allowed:false,reason:'RECORDED_LIVE_DISABLED'};
      // Same full-margin condition as the live client; no extra 5% reserve or virtual top-up.
      const required=BigInt(Math.round(sizing.margin*1e6))*10n**12n;
      if(e.available()<required)return{allowed:false,reason:'ACTUAL_SIZE_MARGIN_RESERVED'};
      return{allowed:true};
    };
    e.entryPolicyReason=function(){const g=m.entryGate();return g.allowed?null:g.reason;};
    const advance=m.advance.bind(m);
    m.advance=function(t,...args){
      Object.assign(m.bridge.api.cfg,t<switchAt?{regLowUsd:60,regHighUsd:82}:{regLowUsd:82,regHighUsd:129});
      return advance(t,...args);
    };
    if(m.id!=='vlad_baseline'){
      installTrainQueue(e,modelCal,{entryTtlMs:setting.entryTtlMs??20000});
      require(path.join(source,'apps/strategy-lab/fast-mark.cjs')).installMarkCache(e);
    }
    if(m.id!=='vlad_baseline'&&m.id!=='train_queue_only')pendingById.set(m.id,installObservation(e,modelCal));
    if(plan.reconcileSignedExpiry)installExpiryReconciliation(e);
    m.hash=crypto.createHash('sha256').update(JSON.stringify({profile:m.profile,plan,modelCal,runnerSha256,experiment:e.experiment,scenario:e.scenario,parent:m.hash})).digest('hex');
    replay.db.prepare('INSERT OR IGNORE INTO sim_strategy_versions VALUES(?,?,?,?)').run(m.hash,m.id,plan.start,JSON.stringify({profile:m.profile,researchOverrides:{...sizing,observationDelay:pendingById.has(m.id)},calibration:setting.calibration??'calibration.json',scenario:e.scenario,runnerSha256}));
    m.save(plan.start-plan.warmupMs);
  }
  const {spawn}=require('node:child_process'),readline=require('node:readline');
  const db=path.resolve(__dirname,'../../data/unified/market.sqlite3');
  const py="import importlib.util,sys; from pathlib import Path; s=importlib.util.spec_from_file_location('tape',sys.argv[1]); m=importlib.util.module_from_spec(s); s.loader.exec_module(m); m.export(Path(sys.argv[2]),int(sys.argv[3]),int(sys.argv[4]),'canonical')";
  const child=spawn('python',['-u','-c',py,path.join(source,'apps/strategy-lab/tape-export.py'),db,String(plan.start-plan.warmupMs),String(plan.end)],{stdio:['ignore','pipe','pipe']});
  let stderr='';child.stderr.on('data',b=>stderr=(stderr+String(b)).slice(-4000));
  const done=new Promise((resolve,reject)=>{child.once('error',reject);child.once('close',resolve);});
  const hash=crypto.createHash('sha256');
  try{
    for await(const line of readline.createInterface({input:child.stdout,crlfDelay:Infinity})){
      hash.update(line+'\n');replay.ingest(JSON.parse(line));
      if(replay.events%200000===0)fs.writeFileSync(path.join(folder,'strategy-progress.json'),JSON.stringify({events:replay.events,through:replay.last,seedOffset,updatedMs:Date.now(),models:replay.models.map(m=>({id:m.id,counts:m.engine.counts,lastPolicy:m.engine.lastEntryPolicyReason}))}));
    }
    const code=await done;if(code!==0)throw new Error('EXPORT_FAILED:'+stderr);
    const report=replay.finish();
    report.inputSha256=hash.digest('hex');report.calibrationSha256=crypto.createHash('sha256').update(fs.readFileSync(path.join(folder,'calibration.json'))).digest('hex');
    report.executionSemantics='Physical execution precedes strategy visibility in train_queue_and_observation; source can act only at observed time';
    report.selected=null;report.runnerSha256=runnerSha256;
    report.executionAssumptions=replay.models.map(m=>({id:m.id,effectiveExperiment:m.engine.experiment,effectiveScenario:m.engine.scenario}));
    report.variants=replay.models.map(m=>({id:m.id,sizing:sizingFor(plan,m.id),summary:{...m.summary(),notionalUsd:sizingFor(plan,m.id).notional,sourceParameters:'reviewed Y2 rules; explicit research sizing, fixed account, no top-ups'},observationPending:pendingById.get(m.id)?.size??0,
      nativePositions:m.bridge.api.positions.size,nativePending:m.bridge.api.pending.length,
      entries:[...m.engine.intents.values()].filter(r=>r.kind==='entry').map(r=>({id:r.id,createdAt:r.createdAt,side:r.q.side,strategy:r.q.strategy,state:r.state,delay:r.entryDelay,cancelRequested:r.cancelRequested??null}))}));
    report.sizing={variants:variants.map(id=>({id,...sizingFor(plan,id)})),virtualTopUpsAllowed:false};
    report.selection='No strategy parameter optimization. Train-only execution model evaluated on chronological holdout.';
    const dest=path.join(folder,'strategy-result-'+seedOffset+'.json');
    fs.writeFileSync(dest,JSON.stringify(report,null,2),{flag:'wx'});
    replay.db.exec("VACUUM INTO '"+path.join(folder,'strategy-'+seedOffset+'.sqlite').replaceAll("'","''")+"'");
    process.stdout.write(JSON.stringify({events:report.inputEvents,models:report.results.map(r=>({id:r.profile.id,counts:r.counts,all:r.all,validation:r.validation}))})+'\n');
  }finally{child.kill();replay.close();}
}
if(require.main===module)run(path.resolve(process.argv[2]),Number(process.argv[3]??0)).catch(e=>{console.error(e);process.exitCode=1;});
module.exports={installObservation,installTrainQueue,sizingFor,installExpiryReconciliation};
