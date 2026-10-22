'use strict';
process.env.TZ='Europe/Moscow';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const {ROOT,SOURCE_ROOT,sha,sourceManifest,createBridge}=require('../../apps/papertrade-shadow/strategy.cjs');
const {ENDPOINT,validateSource,validateEvent,validateEnvelope,LoopbackReader,Continuity}=require('../../apps/papertrade-shadow/live-feed.cjs');
const {virtualEntryLimit,entryAllowed}=require('../../apps/papertrade-shadow/deadline.cjs');
const {IncrementalSession,AssetRouter}=require('../../apps/papertrade-shadow/incremental.cjs');
const {TelemetryPublisher,Audit,createRunDirectory}=require('../../apps/papertrade-shadow/telemetry.cjs');
const {Execution,simulate}=require('../../apps/papertrade-shadow/engine.cjs');
const {plan,validatePlan,run,args}=require('../../apps/papertrade-shadow/realtime.cjs');
const {classifyInterval,scopedEvidence}=require('../../apps/papertrade-shadow/evidence.cjs');
const {forensic}=require('../../apps/papertrade-shadow/forensic.cjs');
const launch=require('../../apps/papertrade-shadow/baseline.json'),base=require('../../apps/papertrade-shadow/scenarios.json');
const {START,tape}=require('./fixtures.cjs');
const scenario={...base.scenarios[1],entryMs:[4000],exitMs:[9000],cancelMs:[500],congestion:0,volatility:0,cancelConflictProbability:1};
const config={...base,realtime:{maxReceiveAgeMs:2500},entryDeadline:{signalTtlMs:15000,safetySeconds:1}};
const locks=()=>[{path:'apps/vchera2/launch-config.json',sha256:sha(fs.readFileSync(path.join(SOURCE_ROOT,'apps/vchera2/launch-config.json')))},...sourceManifest()];
function event({seq=1,t=START,symbol='BTC',venue='hl',kind='market',book=true}={}){
  const bid=symbol==='BTC'?'81799.5':'3000.00',ask=symbol==='BTC'?'81800.5':'3000.01';
  const p=kind==='market'?{bid,ask,frameKind:book?'book':'quote',instrumentId:symbol==='BTC'?0:1,priceScale:symbol==='BTC'?'10':'100',sourceTimestampKnown:true}:{};
  if(kind==='market'&&book){p.bids=[[bid,'1']];p.asks=[[ask,'12']];}
  return {schemaVersion:1,sequence:seq,kind,symbol,venue:kind==='market'?venue:'paper',recv_ms:t,source_ms:t,payload:p,payload_sha256:sha(JSON.stringify(p)),provenance:'direct public WS reused by reviewed dashboard'};
}
function envelope(events=[],{bootId='a',gap=false,lastSequence=events.at(-1)?.sequence??0,firstSequence=1,observedAtMs=START}={}){
  return {schemaVersion:1,mode:'READ_ONLY_RESEARCH_NO_ORDERS',bootId,gap,lastSequence,firstSequence,observedAtMs,events};
}
function rehash(e){e.payload_sha256=sha(JSON.stringify(e.payload));return e;}
function temp(){return fs.mkdtempSync(path.join(ROOT,'reports/acceptance-test-'));}
function engine(c=config){const e=new Execution(c,base.experiments[0],scenario,START);e.bridge=createBridge(e,launch,base.experiments[0],START);return e;}
function pending(e){const q={strategy:'book',side:'S',margin:2100,createdAt:START,px:81800,ctx:{sig:.8,dev:0,spread:0,baseline:0,books:[]}};q.intentId=e.submit(q);e.bridge.api.pending.push(q);return e.intents.get(q.intentId);}

test('exact fixed loopback source; credentials, redirects/query variants, DNS aliases, other paths/ports fail closed',()=>{
  assert.equal(validateSource(ENDPOINT),ENDPOINT);
  for(const s of ['https://127.0.0.1:18762/api/research/events','http://localhost:18762/api/research/events',ENDPOINT+'?after=0',ENDPOINT+'/',ENDPOINT.replace('18762','80'),'http://' + ['synthetic-user','synthetic-password'].join(':') + '@127.0.0.1:18762/api/research/events',ENDPOINT.replace('/research/events','/orders')])assert.throws(()=>validateSource(s),/DENIED/);
  assert.throws(()=>args(['--live','true']));assert.throws(()=>args(['--source',ENDPOINT,'--source',ENDPOINT]));
});
test('offline preloader denies even approved transport; bounded reader configuration rejects unbounded request options',()=>{
  const r=new LoopbackReader();assert.throws(()=>r.get(0),/OFFLINE_NETWORK_DENIED/);
  for(const x of [{limit:257},{timeoutMs:5001},{maxBytes:4194305}])assert.throws(()=>new LoopbackReader(x));
});
test('payload checksum explicitly local not wire, fractional receive clock and missing source clock stay honest',()=>{
  const e=event({t:START+.25});e.source_ms=null;e.payload.sourceTimestampKnown=false;rehash(e);
  const v=validateEvent(e);assert.match(v.hashKind,/NOT-raw-wire/);assert.equal(v.source_ms,null);
  e.payload.bid='81798.0';assert.throws(()=>validateEvent(e),/checksum/);
});
test('book/quote distinction, asset-specific scales and provenance conflicts fail closed; ps is not consulted',()=>{
  const quote=event({book:false});assert.ok(validateEvent(quote));assert.equal(quote.payload.bids,undefined);
  quote.payload.bids=[['81799.5','1']];rehash(quote);assert.throws(()=>validateEvent(quote),/invent depth/);
  const eth=event({symbol:'ETH'});eth.payload.priceScale='10';rehash(eth);assert.throws(()=>validateEvent(eth),/metadata/);
  const wrong=event({venue:'bf'});wrong.provenance='wss://stream.binance.com/stream';assert.throws(()=>validateEvent(wrong),/conflict/);
  const usd=event({venue:'bf'});usd.provenance='wss://fstream.binance.com/stream';usd.payload.ps='NOT-A-VENUE-PROOF';rehash(usd);assert.ok(validateEvent(usd));
});
test('envelope mode/sequence/limit checks cannot admit financial or unordered feeds',()=>{
  assert.throws(()=>validateEnvelope({...envelope(),mode:'LIVE'},128));
  assert.throws(()=>validateEnvelope(envelope([event({seq:2}),event({seq:1})]),128));
  assert.throws(()=>validateEnvelope(envelope([event(),event({seq:2})]),1));
});
test('bootstrap skips buffered history; incremental sequences accepted, boot/gap reset, duplicates remain observations',()=>{
  const c=new Continuity();const first=c.consume(envelope([event()],{lastSequence:20}));
  assert.equal(first.events.length,0);assert.equal(c.cursor,20);assert.equal(first.invalidated,true);
  let b=c.consume(envelope([event({seq:21})]));assert.equal(b.events.length,1);assert.equal(b.continuous,false);
  b=c.consume(envelope([event({seq:21}),event({seq:22})]));assert.equal(b.events.length,1);assert.equal(b.continuous,true);assert.equal(c.counts.duplicates,1);
  b=c.consume(envelope([event()],{bootId:'b'}));assert.equal(b.invalidated,true);assert.equal(b.events.length,0);
  b=c.consume(envelope([],{bootId:'b',gap:true,lastSequence:10}));assert.equal(b.invalidated,true);assert.equal(c.cursor,10);
});
test('silent sequence loss and receive regression invalidate continuity rather than backfill',()=>{
  const c=new Continuity();c.consume(envelope());
  let b=c.consume(envelope([event({seq:2})]));assert.ok(b.invalidated);assert.equal(b.events.length,0);
  c.consume(envelope([event({seq:3,t:START+10})]));
  b=c.consume(envelope([event({seq:4,t:START+5})]));assert.ok(b.invalidated);assert.equal(b.events.length,0);
});
test('immutable absolute entry limit respects the WHOLE final allowed second; equality accepted, +1 second denied',()=>{
  for(const tail of [0,1,500,999]){
    const valid=START+15000+tail,l=virtualEntryLimit({intentId:'i',decidedAtMs:START,signalValidUntilMs:valid,safetySeconds:1});
    assert.ok(l.maxAllowedExecutionMs<valid-1000);assert.ok(entryAllowed(l,l.deadlineSec));assert.equal(entryAllowed(l,l.deadlineSec+1),false);
    assert.throws(()=>{l.deadlineSec++;},TypeError);assert.equal(l.realSignature,false);
  }
});
test('local signal expiry and accepted cancel do not release reservation or alter the original signed validity',()=>{
  const e=engine();try{
    e.scenario={...scenario,cancelConflictProbability:0,cancelAckOnly:true};const r=pending(e),original=r.entryLimit.commitmentSha256;
    r.q.ctx.signalValidUntilMs=START+100000;e.cancel(r.id,'local signal expired');e.now=START+500;e.fire({kind:'cancel',id:r.id});
    e.bridge.step(START+16000);assert.equal(e.reserved,2100n*10n**18n);assert.equal(r.state,'queued');
    assert.equal(r.entryLimit.signalValidUntilMs,START+15000);assert.equal(r.entryLimit.commitmentSha256,original);assert.equal(e.counts.cancelled,0);assert.ok(r.q.localSignalExpired);
  }finally{e.bridge.close();}
});
test('virtual block check rejects execution after deadline; expiry is not reported as successful cancel',()=>{
  const e=engine();try{const r=pending(e);e.now=(r.entryLimit.deadlineSec+1)*1000;e.fire({kind:'entry',id:r.id});
    assert.equal(r.state,'expired');assert.equal(e.counts.entryExpired,1);assert.equal(e.counts.cancelled,0);assert.equal(e.reserved,0n);
  }finally{e.bridge.close();}
});
test('execution before deadline observed after it still becomes exposure; protective exit remains queued',()=>{
  const e=engine();try{
    const r=pending(e),executedAtMs=r.entryLimit.deadlineSec*1000;
    e.mark(START+20000);
    assert.equal(e.observeEntry(r.id,{executedAtMs,observedAtMs:e.now,executionBlockSeconds:r.entryLimit.deadlineSec,priceRaw:818000n}),true);
    assert.equal(e.positions.size,1);assert.equal(e.counts.entryExpired,0);assert.ok(e.counts.lateFill>0);
    const fill=e.ledger.find(x=>x.kind==='entry_fill');assert.equal(fill.observedAtMs,START+20000);assert.equal(fill.executedAtMs,executedAtMs);
    assert.ok(e.ledger.some(x=>x.kind==='close_requested'));assert.equal(e.orders.length,0);
    assert.ok(e.timers.some(x=>x.kind==='exit'&&x.at>e.now));
  }finally{e.bridge.close();}
});
test('ETH is accepted with native scale but stays UNCONFIGURED observe-only and never touches BTC feed/thresholds',()=>{
  const s=new IncrementalSession(config,launch,base.experiments[0],scenario,START);try{
    s.ingest(validateEvent(event({symbol:'ETH'})),START);s.advanceTo(START);
    const x=s.snapshot();assert.equal(x.assets.ETH.readiness,'UNCONFIGURED');assert.equal(x.assets.ETH.counts.book,1);
    assert.equal(x.assets.ETH.btcThresholdsApplied,false);assert.equal(s.engine.counts.submitted,0);assert.ok(Number.isNaN(s.bridge.api.feed.hl.bid));
    assert.equal(x.assets.BTC.inputCounts.market,0);
  }finally{s.close();}
});
test('generic asset routing only enables a strategy consumer with explicit validated-native-profile flag',()=>{
  const r=new AssetRouter(),consumer={ingest(){},snapshot(){return {};}};
  assert.throws(()=>r.register('ETH',consumer,{observeOnly:false}),/validated/);
  assert.doesNotThrow(()=>r.register('ETH',consumer,{observeOnly:false,validatedNativeProfile:true}));
});
function drive(s,start,duration=186000){
  let seq=0;
  for(let dt=0;dt<=duration;dt+=100){
    if(dt%500===0)for(const venue of ['hl','bf'])for(const book of [false,true])s.ingest(validateEvent(event({seq:++seq,t:start+dt,venue,book})),start+dt);
    s.advanceTo(start+dt);
  }
}
test('incremental reviewed BTC source completes real-duration warmup with separate candidates/guards/intents, synthetic probe only',()=>{
  const s=new IncrementalSession(config,launch,base.experiments[0],scenario,START);try{
    s.setContinuity(true);drive(s,START);
    assert.ok(s.engine.tickStats.reasons.warmup>0);assert.ok(s.engine.tickStats.postWarmupTicks>0);assert.ok(s.engine.tickStats.entryEligibleInputTicks>0);
    assert.ok(s.engine.tickStats.rawSignalTicks>0);assert.ok(s.engine.tickStats.candidateEvaluations>0);assert.ok(s.engine.counts.submitted>0);
  }finally{s.close();}
});
test('independent quote/book source clocks accept fresh late-arriving depth without overwriting newer BBO or restarting warmup',()=>{
  const s=new IncrementalSession(config,launch,base.experiments[0],scenario,START);try{
    s.setContinuity(true);
    const q=event({book:false,t:START+100});q.payload.bid='81800.5';q.payload.ask='81801.5';rehash(q);s.ingest(validateEvent(q),START+100);
    const b=event({seq:2,t:START+110});b.source_ms=START+50;s.ingest(validateEvent(b),START+110);
    assert.equal(s.bridge.api.feed.hl.bid,81800.5);assert.equal(s.bridge.api.feed.hl.bookT,START+50);assert.ok(s.bridge.api.feed.hl.bids.length);
    const older=event({seq:3,t:START+120});older.source_ms=START+40;s.ingest(validateEvent(older),START+120);
    assert.equal(s.bridge.api.feed.hl.bookT,START+50);assert.equal(s.engine.inputHealthy,true);assert.equal(s.lastInvalidate,null);assert.equal(s.inputCounts.rejectedMarket,0);
  }finally{s.close();}
});
test('gap clears observed depth and restarts integrity warmup without clearing existing risk',()=>{
  const s=new IncrementalSession(config,launch,base.experiments[0],scenario,START);try{
    s.setContinuity(true);drive(s,START,181000);const risk=s.engine.reserved+s.engine.locked;assert.ok(risk>0n);
    s.invalidate('probe_gap',s.now);assert.equal(s.bridge.api.feed.hl.bookT,0);assert.equal(s.engine.reserved+s.engine.locked,risk);
    s.setContinuity(true);for(const venue of ['hl','bf'])for(const book of [false,true])s.ingest(validateEvent(event({t:s.now,venue,book})),s.now);
    s.advanceTo(s.now+100);assert.ok(s.engine.tickStats.reasons.recoveryWarmup>0);assert.ok(s.engine.integrityWarmUntil>s.now);
  }finally{s.close();}
});
test('missing depth, stale receive, source skew and processing stalls each have visible fail-closed reasons',()=>{
  const s=new IncrementalSession(config,launch,base.experiments[0],scenario,START);try{
    s.setContinuity(true);s.ingest(validateEvent(event({book:false})),START);s.advanceTo(START);
    assert.ok(s.engine.tickStats.reasons.missingDepth>0);
    s.ingest(validateEvent(event({seq:2,t:START})),START+3000);assert.equal(s.inputCounts.rejectedStale,1);
    const f=event({seq:3,t:START+3000});f.source_ms=START+20000;s.ingest(validateEvent(f),START+3000);assert.ok(s.inputCounts.rejectedMarket>0);assert.equal(s.engine.inputHealthy,false);
    const before=s.engine.tickStats.ticks;s.advanceTo(START+9000);assert.equal(s.engine.tickStats.ticks,before+1);assert.ok(s.inputCounts.missedTicks>0);
  }finally{s.close();}
});
test('absolute historical cutoff is never silently moved; explicit research-window override stays labelled',()=>{
  const late=START+5*3600000;
  const respected=new IncrementalSession(config,launch,base.experiments[0],scenario,late);try{
    respected.setContinuity(true);drive(respected,late);assert.equal(respected.engine.counts.submitted,0);assert.ok(respected.engine.tickStats.reasons.cutoff>0);
    assert.equal(respected.bridge.cutoff.policy,'respect-production');
  }finally{respected.close();}
  const c={...config,researchWindow:{startMs:late,endMs:late+360000,cutoffPolicy:'research-window',acknowledgeResearchOnlyOverride:true}};
  const explicit=new IncrementalSession(c,launch,base.experiments[0],scenario,late);try{
    explicit.setContinuity(true);drive(explicit,late);assert.ok(explicit.engine.counts.submitted>0);
    assert.notEqual(explicit.bridge.cutoff.effectiveCutoffMs,explicit.bridge.cutoff.baselineCutoffMs);assert.equal(explicit.bridge.cutoff.productionConfigModified,false);
  }finally{explicit.close();}
});
test('offline ETH events cannot contaminate BTC baseline or fabricate ETH readiness',async()=>{
  const events=tape({end:1000}).map(x=>({...x,market:'ETH',payload:{...x.payload,bid:'3000',ask:'3001'}}));
  const r=await simulate(events,config,launch,base.experiments[0],scenario);
  assert.equal(r.summary.counts.submitted,0);assert.equal(r.summary.assetScope.observed.BTC,0);assert.ok(r.summary.assetScope.observed.ETH>0);
});
test('telemetry atomic writes, publication scope, symlink denial and audit cap preserve the research boundary',()=>{
  const out=temp();try{
    const p=new TelemetryPublisher(out);for(let i=0;i<10;i++){p.publish({runId:'probe',i,financialPerformance:'simulated'});assert.equal(JSON.parse(fs.readFileSync(path.join(out,'telemetry.json'),'utf8')).i,i);}
    assert.equal(fs.readdirSync(out).filter(x=>x.endsWith('.tmp')).length,0);
    assert.throws(()=>new TelemetryPublisher(path.join(ROOT,'apps/vchera2')),/outside/);
    fs.unlinkSync(path.join(out,'telemetry.json'));fs.symlinkSync(path.join(ROOT,'apps/papertrade-shadow/baseline.json'),path.join(out,'telemetry.json'));
    assert.throws(()=>p.publish({}),/Symlink/);
    const a=new Audit(out,1024);assert.throws(()=>{for(let i=0;i<20;i++)a.append('probe',{x:'x'.repeat(200)},START);},/SIZE_BOUND/);a.close();assert.ok(fs.statSync(path.join(out,'audit.jsonl')).size<=1024);
    assert.throws(()=>createRunDirectory(path.join(ROOT,'apps/vchera2/prohibited-test')),/outside/);
  }finally{fs.rmSync(out,{recursive:true,force:true});}
});
test('own/Vlad holding periods and journal decisions do not become exact queue latency or cross-asset calibration',()=>{
  const held=classifyInterval({owner:'Vlad',asset:'BTC',notional:1000,openedAtMs:1000,closedAtMs:8000});
  assert.equal(held.intervalKind,'holding-period');assert.equal(held.queueCalibrationEligible,false);
  const j=classifyInterval({owner:'own',asset:'BTC',decisionAtMs:1000,executedAtMs:66500,ambiguity:'fallback match'});assert.equal(j.queueCalibrationEligible,false);
  const scope=scopedEvidence([{owner:'Vlad',asset:'ETH',notional:1000,submittedAtMs:1000,executedAtMs:2000,intentId:'proof',strongIntentAttribution:true}],{owner:'own',asset:'BTC',notional:1260000});
  assert.equal(scope.eligibleExactScope,0);assert.equal(scope.objectiveQueueModel,'UNIDENTIFIABLE');
});
test('precommitted run requires 300..900s, explicit cutoff policy and separate ETH configuration; short probe is never acceptance',()=>{
  const p=plan({source:ENDPOINT,seconds:360,cutoffPolicy:'research-window',now:START});assert.ok(p.researchWindow.acknowledgeResearchOnlyOverride);
  assert.doesNotThrow(()=>validatePlan(p,ENDPOINT,360,START));assert.throws(()=>validatePlan(p,ENDPOINT,30,START));
  const q=structuredClone(p);q.assets.ETH.mode='simulated';assert.throws(()=>validatePlan(q,ENDPOINT,360,START),/ETH/);
  q.assets.ETH.mode='observe-only';q.researchWindow.acknowledgeResearchOnlyOverride=false;assert.throws(()=>validatePlan(q,ENDPOINT,360,START),/acknowledged/);
});
test('bounded smoke processes synthetic feed and publishes telemetry BEFORE window completion; no retrospective sleep/replay',async()=>{
  const parent=temp(),out=path.join(parent,'live-probe');try{
    const start=Date.now(),p=plan({source:ENDPOINT,seconds:300,cutoffPolicy:'respect-production',now:start});p.realtime.publishMs=100;
    let seq=0;const reader={requests:0,async get(){this.requests++;await new Promise(r=>setTimeout(r,20));const t=Date.now();return envelope(['BTC','ETH'].flatMap(symbol=>[false,true].map(book=>event({seq:++seq,t,symbol,book}))),{observedAtMs:t,lastSequence:seq});}};
    const job=run({config:p,locks:locks(),source:ENDPOINT,seconds:1,out,smoke:true,reader});
    await new Promise(r=>setTimeout(r,400));
    const mid=JSON.parse(fs.readFileSync(path.join(out,'telemetry.json'),'utf8'));assert.equal(mid.status,'RUNNING');assert.ok(mid.input.continuity.events>0);assert.ok(mid.assets.ETH.counts.book>0);
    const final=await job;assert.equal(final.status,'SMOKE_ONLY');assert.equal(final.acceptance.completeWarmup,false);assert.equal(final.financialMutations,0);
  }finally{fs.rmSync(parent,{recursive:true,force:true});}
});
test('transport failure produces atomic ERROR summary and terminates finite run without financial mutation',async()=>{
  const parent=temp(),out=path.join(parent,'error-probe');try{
    const p=plan({source:ENDPOINT,seconds:300,cutoffPolicy:'respect-production'});
    await assert.rejects(()=>run({config:p,locks:locks(),source:ENDPOINT,seconds:1,out,smoke:true,reader:{requests:1,async get(){throw new Error('probe denied transport');}}}),/failed closed/);
    const x=JSON.parse(fs.readFileSync(path.join(out,'summary.json'),'utf8'));assert.equal(x.status,'ERROR');assert.equal(x.financialMutations,0);
  }finally{fs.rmSync(parent,{recursive:true,force:true});}
});
// Historical account settlement regressions require private local datasets and are not exported.
