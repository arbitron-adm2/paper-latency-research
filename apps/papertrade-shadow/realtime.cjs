'use strict';
process.env.TZ='Europe/Moscow';
const fs=require('node:fs'),path=require('node:path');
const {ROOT,sha}=require('./strategy.cjs');
const {checkLockRecords,validateConfig}=require('./cli.cjs');
const {ENDPOINT,validateSource,LoopbackReader,Continuity}=require('./live-feed.cjs');
const {IncrementalSession}=require('./incremental.cjs');
const {allowedRoots,createRunDirectory,writeNew,TelemetryPublisher,Audit}=require('./telemetry.cjs');
const APP=__dirname;
const sleep=ms=>new Promise(resolve=>setTimeout(resolve,ms));
function readResearchJSON(file){
  const p=fs.realpathSync(path.resolve(file));
  const roots=[APP,...allowedRoots(),path.join(ROOT,'tests/papertrade-shadow')];
  if(path.extname(p)!=='.json'||p.includes('/.runtime/')||!roots.some(r=>p.startsWith(r+path.sep))||fs.statSync(p).size>2097152)throw new Error('Research JSON read outside allowlist');
  return JSON.parse(fs.readFileSync(p,'utf8'));
}
function args(argv){
  const out={};
  for(let i=0;i<argv.length;i+=2){const k=argv[i]?.slice(2);
    if(!argv[i]?.startsWith('--')||!['source','seconds','config','source-lock','out','out-config','cutoff-policy'].includes(k)||!argv[i+1]||argv[i+1].startsWith('--')||k in out)throw new Error('Expected unique explicit --option value');out[k]=argv[i+1];}
  return out;
}
function plan({source,seconds,cutoffPolicy,now=Date.now()}){
  validateSource(source);
  if(!Number.isInteger(seconds)||seconds<300||seconds>900||!['respect-production','research-window'].includes(cutoffPolicy))throw new Error('Plan requires 300..900 seconds and an explicit cutoff policy');
  const research=readResearchJSON(path.join(APP,'scenarios.json'));
  return {schemaVersion:1,mode:'READ_ONLY_RESEARCH_NO_ORDERS',createdAtMs:now,durationSeconds:seconds,source,
    researchWindow:{startMs:now,endMs:now+seconds*1000+30000,cutoffPolicy,acknowledgeResearchOnlyOverride:cutoffPolicy==='research-window',
      rationale:cutoffPolicy==='research-window'?'Explicit standalone simulated research window; historical production cutoff is NOT modified':'Keep production absolute cutoff; suppression is separately counted'},
    entryDeadline:{signalTtlMs:15000,safetySeconds:1,contractEqualityAllowed:true,realSignature:false},
    realtime:{pollMs:50,publishMs:1000,limit:256,timeoutMs:2000,maxResponseBytes:2097152,maxReceiveAgeMs:2500,maxServerClockSkewMs:5000,auditMaxBytes:Math.min(2147483648,seconds*2*1024*1024)},
    researchConfig:{...research,entryDeadline:{signalTtlMs:15000,safetySeconds:1}},
    experimentId:'baseline',scenarioId:'observed-sized-latency-hypothesis',
    evidence:{factual:'Observed local market receive/input lags and canonical provider executions',
      empiricalTransfer:'Own journal decision-to-execution advisory pools; not verified submit-to-fill; BTC baseline scope only',
      pureHypotheses:['conflict probability','congestion and volatility multipliers','volume priority','unobserved tails','execution price proxy'],
      objectiveQueueModel:'UNIDENTIFIABLE',cashPayoutVerified:false},
    assets:{BTC:{mode:'simulated',profile:'reviewed-baseline'},ETH:{mode:'observe-only',readiness:'UNCONFIGURED',btcThresholdsApplied:false}},
    initialConditions:'Cold start; no imported production EV/history/high-water state',financialMutationsAllowed:false};
}
function validatePlan(c,source,seconds,now,{smoke=false}={}){
  validateSource(source);
  if(c.schemaVersion!==1||c.mode!=='READ_ONLY_RESEARCH_NO_ORDERS'||c.source!==source||c.financialMutationsAllowed!==false||!Number.isInteger(c.durationSeconds)||c.durationSeconds<300||c.durationSeconds>900)throw new Error('Explicit valid precommitted research plan required');
  if(!Number.isInteger(seconds)||(smoke?seconds<1||seconds>10:seconds<300||seconds>900||seconds!==c.durationSeconds))throw new Error('Acceptance duration 300..900s; smoke 1..10s only');
  const w=c.researchWindow,r=c.realtime;
  if(!w||!['respect-production','research-window'].includes(w.cutoffPolicy)||w.cutoffPolicy==='research-window'&&w.acknowledgeResearchOnlyOverride!==true||!Number.isSafeInteger(w.startMs)||!Number.isSafeInteger(w.endMs)||now<w.startMs||now+seconds*1000>w.endMs||w.endMs-w.startMs>930000)throw new Error('Research window expired, too broad or unacknowledged');
  if(c.entryDeadline?.realSignature!==false||c.entryDeadline?.contractEqualityAllowed!==true||!Number.isInteger(c.entryDeadline?.signalTtlMs)||c.entryDeadline.signalTtlMs<3000||c.entryDeadline.signalTtlMs>15000||!Number.isInteger(c.entryDeadline.safetySeconds)||c.entryDeadline.safetySeconds<0||c.entryDeadline.safetySeconds>10)throw new Error('Independent bounded absolute entry deadline required');
  if(!r||!Number.isInteger(r.pollMs)||r.pollMs<50||r.pollMs>1000||!Number.isInteger(r.publishMs)||r.publishMs<100||r.publishMs>5000||!Number.isInteger(r.maxReceiveAgeMs)||r.maxReceiveAgeMs<500||r.maxReceiveAgeMs>5000||!Number.isInteger(r.maxServerClockSkewMs)||r.maxServerClockSkewMs<100||r.maxServerClockSkewMs>5000)throw new Error('Bounded realtime policy required');
  if(!Number.isInteger(r.limit)||r.limit<1||r.limit>256||!Number.isInteger(r.timeoutMs)||r.timeoutMs<100||r.timeoutMs>5000||!Number.isInteger(r.maxResponseBytes)||r.maxResponseBytes<1024||r.maxResponseBytes>4194304||!Number.isInteger(r.auditMaxBytes)||r.auditMaxBytes<1024||r.auditMaxBytes>2147483648)throw new Error('Response/cursor/audit bounds required');
  if(c.assets?.ETH?.mode!=='observe-only'||c.assets.ETH.readiness!=='UNCONFIGURED'||c.assets.ETH.btcThresholdsApplied!==false)throw new Error('No validated ETH native config: observe-only required');
  if(c.assets?.BTC?.mode!=='simulated')throw new Error('BTC must be simulated');
  const x=c.researchConfig;
  if(!x||!Number.isInteger(x.seed)||!x.scenarios?.length||!x.experiments?.length||c.experimentId!=='baseline'||!x.experiments.some(e=>e.id===c.experimentId)||!x.scenarios.some(s=>s.id===c.scenarioId))throw new Error('Reviewed baseline research engine config required');
  validateConfig(x);
  const e=x.experiments.find(e=>e.id===c.experimentId);
  if(e.margin!==2100||e.leverage!==600||String(e.balance)!=='2300')throw new Error('Realtime BTC baseline sizing must not drift');
}
async function run({config,locks,source,seconds,out,smoke=false,reader=null,clock=Date.now}){
  const started=clock();validatePlan(config,source,seconds,started,{smoke});checkLockRecords(locks);
  const launch=readResearchJSON(path.join(APP,'baseline.json'));
  const research={...config.researchConfig,realtime:config.realtime,researchWindow:config.researchWindow,entryDeadline:config.entryDeadline};
  const experiment=research.experiments.find(x=>x.id===config.experimentId),scenario=research.scenarios.find(x=>x.id===config.scenarioId);
  const dir=createRunDirectory(out),runId=path.basename(dir),publisher=new TelemetryPublisher(dir),audit=new Audit(dir,config.realtime.auditMaxBytes);
  const session=new IncrementalSession(research,launch,experiment,scenario,started),continuity=new Continuity();
  const feed=reader??new LoopbackReader({source,limit:config.realtime.limit,timeoutMs:config.realtime.timeoutMs,maxBytes:config.realtime.maxResponseBytes});
  const end=started+seconds*1000;let status='RUNNING',failure=null,lastPublish=0,lastObservedAtMs=null,lastConnection=null,flushedLedger=0,stop=false;
  writeNew(dir,'plan.json',config);writeNew(dir,'source-lock.json',locks);writeNew(dir,'baseline.json',launch);
  function flush(now){while(flushedLedger<session.engine.ledger.length){audit.append('simulated_execution',session.engine.ledger[flushedLedger],now);flushedLedger++;}}
  function snapshot(now){
    const warmupElapsed=now>=Math.max(started+180000,session.engine.integrityWarmUntil),f=session.bridge.api.feed;
    const freshDepth=['hl','bf'].every(v=>f[v].bids.length&&f[v].asks.length&&now-f[v].bookT<=(v==='hl'?10000:3000)&&now-f[v].t<=session.bridge.api.cfg.maxFeedAgeMs);
    return {schemaVersion:1,runId,mode:'READ_ONLY_RESEARCH_NO_ORDERS',status,smokeOnly:smoke,generatedAtMs:now,startedAtMs:started,plannedEndMs:end,
    elapsedMs:now-started,sourceURL:source,financialMutations:0,realSignatures:0,financialPerformance:'simulated',
    input:{bootId:continuity.bootId,lastSequence:continuity.cursor,lastEvent:session.lastInput,lastObservedAtMs,
      eventAgeMs:session.lastInput?now-session.lastInput.recv_ms:null,continuity:continuity.counts,connection:lastConnection,
      payloadHashSemantics:'canonical local JSON payload; NOT raw wire hash',networkRequests:feed.requests??null},
    ...session.snapshot(now),failure,acceptance:{warmupElapsed,completeWarmup:warmupElapsed&&freshDepth&&session.engine.inputHealthy&&(session.engine.tickStats?.entryEligibleInputTicks??0)>0,
      fullCurrentBTCDepth:freshDepth,
      queueModelIdentified:false,cashPayoutVerified:false,performanceVerdict:'INCONCLUSIVE'}};}
  function heartbeat(){
    const now=Math.min(clock(),end);session.advanceTo(now);flush(now);
    if(now-lastPublish>=config.realtime.publishMs){publisher.publish(snapshot(now));lastPublish=now;}
  }
  publisher.publish(snapshot(started));
  // A finite heartbeat runs while HTTP is awaited. It does NOT replay missing
  // historical ticks or wait until the capture window ends to process inputs.
  const timer=setInterval(()=>{if(stop)return;try{heartbeat();}catch(e){failure=e.message;stop=true;}},100);
  try{
    while(!stop&&clock()<end){
      let response;
      try{response=await feed.get(continuity.cursor);}catch(e){session.invalidate('feed_request_failed',Math.min(clock(),end));audit.append('feed_error',{message:e.message},clock());throw e;}
      const now=clock();if(now>end){audit.append('late_response_discarded',{afterDeadline:true},now);break;}
      lastObservedAtMs=response.observedAtMs;lastConnection=response.connection??null;
      const batch=continuity.consume(response);
      for(const notice of batch.notices)audit.append('input_continuity',notice,now);
      if(batch.invalidated)session.invalidate(batch.notices[0]?.kind??'input_gap',now);
      const clockOk=Math.abs(now-response.observedAtMs)<=config.realtime.maxServerClockSkewMs;
      if(!clockOk)session.invalidate('server_client_clock_skew',now);
      session.setContinuity(batch.continuous&&clockOk);
      for(const e of batch.events){audit.append('observed_event',{event:e},now);session.ingest(e,now);}
      session.advanceTo(now);flush(now);
      if(now-lastPublish>=config.realtime.publishMs){publisher.publish(snapshot(now));lastPublish=now;}
      await sleep(Math.min(config.realtime.pollMs,Math.max(0,end-clock())));
    }
    if(failure)throw new Error(failure);
    checkLockRecords(locks);status=smoke?'SMOKE_ONLY':'COMPLETE';
  }catch(e){failure=e.message;status='ERROR';session.invalidate('run_error_retained_risk',session.now);}
  finally{
    stop=true;clearInterval(timer);
    const now=Math.max(session.now,Math.min(clock(),end));
    try{session.advanceTo(now,{tick:false});flush(now);}catch(e){failure=failure??e.message;status='ERROR';}
    const final=snapshot(now);publisher.publish(final);writeNew(dir,'summary.json',final);
    writeNew(dir,'closed-simulated-positions.json',session.engine.orders);
    writeNew(dir,'open-simulated-intents.json',[...session.engine.intents.values()].map(x=>({id:x.id,kind:x.kind,state:x.state,createdAtMs:x.createdAt,entryLimit:x.entryLimit??null,positionId:x.positionId??null})));
    audit.close();session.close();
  }
  if(failure)throw new Error('Realtime run failed closed: '+failure);
  return JSON.parse(fs.readFileSync(path.join(dir,'summary.json'),'utf8'));
}
async function main(argv=process.argv.slice(2)){
  const [command,...rest]=argv,o=args(rest),source=o.source;
  validateSource(source);
  if(command==='plan'){
    const config=plan({source,seconds:Number(o.seconds),cutoffPolicy:o['cutoff-policy']});
    if(!o['out-config'])throw new Error('--out-config required');
    const p=path.resolve(o['out-config']),parent=fs.realpathSync(path.dirname(p));
    if(!allowedRoots().some(r=>parent===r||parent.startsWith(r+path.sep)))throw new Error('Plan outside allowed research roots');
    writeNew(parent,path.basename(p),config);console.log(JSON.stringify({plan:p,source,seconds:config.durationSeconds,cutoffPolicy:config.researchWindow.cutoffPolicy}));return;
  }
  if(!['run','smoke'].includes(command)||!o.config||!o['source-lock'])throw new Error('run/smoke require --config --source-lock --source --seconds --out');
  const result=await run({config:readResearchJSON(o.config),locks:readResearchJSON(o['source-lock']),source,seconds:Number(o.seconds),out:o.out,smoke:command==='smoke'});
  console.log(JSON.stringify({runId:result.runId,status:result.status,events:result.input.continuity.events,financialMutations:0,telemetry:path.join(o.out,'telemetry.json')}));
}
if(require.main===module)main().catch(e=>{console.error(e.message);process.exitCode=1;});
module.exports={main,plan,validatePlan,run,readResearchJSON,args};
