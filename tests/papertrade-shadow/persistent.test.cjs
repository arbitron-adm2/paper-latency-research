'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const {EventEmitter}=require('node:events');
const {DatabaseSync}=require('node:sqlite');
const {ROOT,sha}=require('../../apps/papertrade-shadow/strategy.cjs');
const {PersistentObserver}=require('../../apps/papertrade-shadow/persistent-session.cjs');
const {PersistentService,parseOptions}=require('../../apps/papertrade-shadow/service.cjs');
const {History,queryHistory,DB_NAME}=require('../../apps/papertrade-shadow/history.cjs');
const {RawRetention}=require('../../apps/papertrade-shadow/raw-retention.cjs');
const {buildProfile}=require('../../apps/papertrade-shadow/persistent-profile.cjs');
const {captureEngine,hasRisk}=require('../../apps/papertrade-shadow/engine-state.cjs');
const {virtualEntryLimit}=require('../../apps/papertrade-shadow/deadline.cjs');
const C=require('../../apps/papertrade-shadow/state-codec.cjs');
const {loadR3}=require('../../apps/papertrade-shadow/r3-observer.cjs');
const START=Date.parse('2026-10-20T08:30:00Z'),UNIT=10n**18n;
fs.mkdirSync(path.join(ROOT,'reports'),{recursive:true});
function temp(){const parent=fs.mkdtempSync(path.join(ROOT,'reports/persist-unit-'));return {parent,dir:path.join(parent,'state')};}
function cleanup(x,o){try{if(o&&!o.history.closed)o.stop('UNIT_CLEANUP',o.session.now);}finally{fs.rmSync(x.parent,{recursive:true,force:true});}}
function market(seq,t,venue,book=false,strong=false,symbol='BTC'){
  const bid=symbol==='BTC'?'81799.5':'3000.00',ask=symbol==='BTC'?'81800.5':'3000.01';
  const payload={bid,ask,frameKind:book?'book':'quote',instrumentId:symbol==='BTC'?0:1,priceScale:symbol==='BTC'?'10':'100',sourceTimestampKnown:true};
  if(book){payload.bids=[[bid,'1']];payload.asks=[[ask,strong?'12':'1']];}
  return {schemaVersion:1,sequence:seq,kind:'market',symbol,venue,recv_ms:t,source_ms:t,payload,payload_sha256:sha(JSON.stringify(payload)),provenance:'synthetic persistent regression, NOT performance'};
}
function pressure(seq,t){
  const top=3+Math.floor((t-START)/1000),blocks=[top-2,top-1,top].map(n=>({number:n,timestampMs:START+(n-3)*1000,papertradeGasUsed:1000000,kind:'ordinary'}));
  const payload={snapshot:{ok:true,stale:false,ts:t,queueRevision:String(seq),totalPendingGas:'0',totalQueuedGas:'0',totalSubmittedGas:'0',pendingCount:0,chain:{ok:true,stale:false,blocks}}};
  return {schemaVersion:1,sequence:seq,kind:'pressure',symbol:null,venue:'paper',recv_ms:t,source_ms:t,payload,payload_sha256:sha(JSON.stringify(payload)),provenance:'synthetic public pressure'};
}
function envelope(events,t,{boot='unit-boot',first=1,last=events.at(-1)?.sequence??0,gap=false}={}){return {schemaVersion:1,mode:'READ_ONLY_RESEARCH_NO_ORDERS',bootId:boot,observedAtMs:t,firstSequence:first,lastSequence:last,gap,events};}
function init(o){o.batch(envelope([],o.session.now),o.session.now);}
function drive(o,duration,{strong=false,from=START,seq=o.continuity.cursor}={}){
  for(let dt=0;dt<=duration;dt+=100){const now=from+dt;
    if(dt%500===0){const events=[];for(const venue of ['hl','bf'])for(const book of [false,true])events.push(market(++seq,now,venue,book,strong));
      if(dt%1000===0)events.push(pressure(++seq,now));o.batch(envelope(events,now),now);}else o.batch(envelope([],now,{last:seq}),now);
    o.heartbeat(now);
  }return seq;
}
function seedPending(o,{at=o.session.now,delay=6000}={}){
  const e=o.session.engine,id='shadow-entry-'+(++e.seq),q={strategy:'book',side:'S',margin:2100,createdAt:at,px:81800,ctx:{sig:.8,dev:0,spread:0,baseline:0,books:[]},intentId:id,live:true};
  const r={id,kind:'entry',state:'queued',createdAt:at,margin:2100n*UNIT,notional:1260000n*UNIT,q,entryLimit:virtualEntryLimit({intentId:id,decidedAtMs:at,signalValidUntilMs:at+15000})};
  e.intents.set(id,r);e.reserved=2100n*UNIT;e.counts.submitted++;e.schedule(at+delay,'entry',id);o.session.bridge.api.bindPending(e.intents);e.note('UNIT_SYNTHETIC_PENDING_NOT_PERFORMANCE',{intentId:id});return r;
}

test('persistent profile fixed>1M, no size alternatives, no LIVE/custom knobs, original baseline lock retained',()=>{
  const p=buildProfile();assert.equal(p.notionalUsd,1260000);assert.ok(p.notionalUsd>1000000);assert.equal(p.experiment.margin,2100);assert.equal(p.experiment.leverage,600);assert.equal(p.config.experiments.length,1);
  assert.equal(p.assets.ETH,'SIM_NATIVE_RULES_LATENCY_UNVALIDATED');assert.equal(p.topUpsAllowed,false);assert.throws(()=>{p.experiment.leverage=1000;},TypeError);
  for(const k of ['margin','leverage','capital','settings','live','model','reset-pnl'])assert.throws(()=>parseOptions(['--state-dir','x','--'+k,'1']),/NO_USER_KNOBS/);
});
test('lossless bounded codec handles exact cash, nonfinite feeds, maps/sets and rejects eval/prototypes/cycles',()=>{
  const v={cash:2300000000000000000000n,n:NaN,i:Infinity,map:new Map([['x',new Set([1,2])]]),u:undefined};const s=C.parse(C.stringify(v));
  assert.equal(s.cash,v.cash);assert.ok(Number.isNaN(s.n));assert.equal(s.i,Infinity);assert.deepEqual([...s.map.get('x')],[1,2]);assert.equal(s.u,undefined);
  assert.throws(()=>C.stringify(()=>{}));const cycle={};cycle.x=cycle;assert.throws(()=>C.stringify(cycle));assert.throws(()=>C.parse('{"t":"object","v":[["__proto__",{}]]}'));
});
test('single observer accumulates beyond900s with original warmup/regime/EV origin, immutable forecasts and all-event analysis',()=>{
  const x=temp();let o;try{
    o=new PersistentObserver({stateDir:x.dir,clock:()=>START});init(o);drive(o,1000000,{strong:true});o.flush(START+1000000);
    const s=o.snapshot(START+1000000),native=o.session.bridge.checkpointState();
    assert.equal(o.session.startMs,START);assert.equal(native.native.startedAt,START);assert.equal(s.assets.BTC.warmup.initialUntilMs,START+180000);assert.ok(s.runtime.stats.events>7000);
    assert.ok(o.session.engine.tickStats.postWarmupTicks>7000);assert.ok(s.history.totals.market_seconds>1000);assert.ok(s.history.totals.sim_forecasts>500);assert.ok(s.history.totals.public_evidence>500);assert.ok(s.history.totals.sim_decisions>0);
    assert.equal(native.nativeJournal,'PRIVATE_SIM_HISTORY_NOT_REAL_VCHERA2_DB');assert.equal(native.tables,null);assert.ok(native.native.mbuf.length>1000);assert.ok(native.native.regime);
    const row=o.history.db.prepare('SELECT * FROM sim_forecasts LIMIT 1').get();const original=row.payload_json;
    o.history.forecast({...C.parse(row.payload_json),bootId:row.boot,sequence:row.sequence});assert.equal(o.history.db.prepare('SELECT payload_json FROM sim_forecasts WHERE boot=? AND sequence=?').get(row.boot,row.sequence).payload_json,original);
    assert.equal(s.financialMutations,0);assert.equal(s.acceptance.performanceVerdict,'INCONCLUSIVE');assert.equal(s.assets.ETH.readiness,'UNCONFIGURED');
    assert.equal(o.session.engine.balance,2300n*UNIT+o.session.engine.realised);
  }finally{cleanup(x,o);}
});
test('graceful short restart preserves pending margin, RNG, cursor, regime/rolling history and completed warmup',()=>{
  const x=temp();let o;try{
    o=new PersistentObserver({stateDir:x.dir,clock:()=>START});init(o);const seq=drive(o,181000,{strong:true});
    assert.ok(o.session.engine.reserved>0n,'reviewed baseline submits in synthetic post-warmup probe');
    const before=o.capture(),run=o.runId;o.stop('UNIT_SHORT_STOP',START+181000);
    o=new PersistentObserver({stateDir:x.dir,clock:()=>START+181500});
    const events=[];let n=seq;for(const venue of ['hl','bf'])for(const book of [false,true])events.push(market(++n,START+181500,venue,book,true));
    o.batch(envelope(events,START+181500),START+181500);o.heartbeat(START+181500);
    assert.equal(o.runId,run);assert.equal(o.recovery.mode,'EXACT_SHORT_RESTART');assert.equal(o.recovery.restoredWarmup,true);
    assert.equal(o.session.startMs,START);assert.equal(o.session.engine.reserved,before.session.engine.reserved);assert.equal(o.session.engine.rng.state,before.session.engine.rngState);
    assert.ok(o.session.bridge.checkpointState().native.mbuf.length>=before.session.bridge.native.mbuf.length);assert.equal(o.session.bridge.api.pending.length,1);
    assert.equal(o.snapshot(START+181500).service.restarts,1);assert.equal(o.history.db.prepare('SELECT COUNT(*) n FROM sim_runs').get().n,1);
  }finally{cleanup(x,o);}
});
test('known open position and native journal survive compatible positionless-market short restart without becoming flat',()=>{
  const x=temp();let o;try{
    o=new PersistentObserver({stateDir:x.dir,clock:()=>START});init(o);const r=seedPending(o);
    const e=o.session.engine;e.mark(START+1000);o.session.now=e.now;o.session.nextTick=e.now+100;
    assert.equal(e.observeEntry(r.id,{executedAtMs:e.now,observedAtMs:e.now,executionBlockSeconds:Math.floor(e.now/1000),priceRaw:818000n}),true);
    assert.equal(e.locked,2100n*UNIT);o.stop('UNIT_OPEN_STOP',START+1000);
    o=new PersistentObserver({stateDir:x.dir,clock:()=>START+1500});o.batch(envelope([],START+1500,{last:0}),START+1500);
    assert.equal(o.recovery.mode,'EXACT_SHORT_RESTART');assert.equal(o.session.engine.positions.size,1);assert.equal(o.session.engine.locked,2100n*UNIT);assert.equal(o.session.bridge.api.positions.size,1);
    assert.equal(o.history.db.prepare("SELECT COUNT(*) n FROM btcy2_lag_trades WHERE status='open'").get().n,1);
  }finally{cleanup(x,o);}
});
test('due during downtime is censored, not filled at a fresh restart quote; unresolved recovery cannot reset risk',()=>{
  const x=temp();let o;try{
    o=new PersistentObserver({stateDir:x.dir,clock:()=>START});init(o);const r=seedPending(o,{delay:100});o.stop('UNIT_STOP',START);
    o=new PersistentObserver({stateDir:x.dir,clock:()=>START+500});o.batch(envelope([],START+500,{last:0}),START+500);o.heartbeat(START+500);
    assert.equal(o.session.engine.reserved,2100n*UNIT);assert.equal(o.session.engine.intents.get(r.id).state,'outcome_unknown');assert.equal(o.session.engine.counts.filled,0);
    assert.ok(o.session.engine.suspendedTimers.length);assert.ok(o.recovery.requiresExplicitRecovery);assert.throws(()=>o.explicitRecovery('RESEARCH_INPUT_RECOVERY_NO_RESET'),/UNRESOLVED_RISK/);
  }finally{cleanup(x,o);}
});
for(const mode of ['corrupt','model-mismatch','native-journal-change'])test('checkpoint '+mode+' preserves cash/reservation and quarantines new virtual entries',()=>{
  const x=temp();let o;try{
    o=new PersistentObserver({stateDir:x.dir,clock:()=>START});init(o);const r=seedPending(o);o.stop('UNIT_STOP',START);
    const db=new DatabaseSync(path.join(x.dir,DB_NAME));
    if(mode==='corrupt')db.exec("UPDATE sim_checkpoints SET sha256='corrupt'");
    else if(mode==='model-mismatch')db.exec("UPDATE sim_checkpoints SET model_hash='different-model'");
    else db.exec("INSERT INTO sim_meta(key,value) VALUES('native_epoch','1') ON CONFLICT(key) DO UPDATE SET value=CAST(value AS INTEGER)+1");
    db.close();o=new PersistentObserver({stateDir:x.dir,clock:()=>START+500});
    assert.equal(o.session.engine.balance,2300n*UNIT);assert.equal(o.session.engine.reserved,2100n*UNIT);assert.equal(o.session.engine.intents.get(r.id).state,'outcome_unknown');assert.equal(o.session.engine.accountingQuarantined,true);
    assert.equal(o.session.engine.submit({strategy:'book',side:'S',margin:2100,ctx:{}}),null);assert.ok(o.recovery.requiresExplicitRecovery);
  }finally{cleanup(x,o);}
});
test('source boot change and actual sequence gap keep pending risk and require repeat input warmup',()=>{
  const x=temp();let o;try{
    o=new PersistentObserver({stateDir:x.dir,clock:()=>START});init(o);const r=seedPending(o);const margin=o.session.engine.reserved;
    o.batch(envelope([],START+100,{boot:'new-boot',first:1,last:100}),START+100);
    assert.equal(o.session.engine.reserved,margin);assert.equal(o.session.engine.intents.get(r.id).state,'outcome_unknown');assert.ok(o.session.engine.integrityWarmUntil>=START+180100);
    o.batch(envelope([],START+200,{boot:'new-boot',first:1000,last:2000,gap:true}),START+200);
    assert.equal(o.session.engine.reserved,margin);assert.ok(o.recovery.requiresExplicitRecovery);assert.ok(o.history.hasRisk());
  }finally{cleanup(x,o);}
});
test('fixed notional gate rejects <=1M and nonpreset alternatives before intent creation, without topup',()=>{
  const x=temp();let o;try{
    o=new PersistentObserver({stateDir:x.dir,clock:()=>START});const e=o.session.engine;
    for(const margin of [10,1000,1666,1800,2099])assert.equal(e.submit({strategy:'book',side:'S',margin,ctx:{}}),null);
    assert.equal(e.counts.submitted,0);assert.equal(e.intents.size,0);assert.equal(e.balance,2300n*UNIT);
    assert.ok(e.ledger.some(r=>r.reason==='NOTIONAL_NOT_STRICTLY_GREATER_THAN_1M'));
    assert.ok(e.ledger.some(r=>r.reason==='FIXED_BTC_2100_X_600_PRESET_REQUIRED'));
  }finally{cleanup(x,o);}
});
test('capital exhaustion and source high-water stop retain PnL/cash and still record raw candidates/reasons across restart',()=>{
  const x=temp();let o;try{
    o=new PersistentObserver({stateDir:x.dir,clock:()=>START});init(o);const e=o.session.engine;
    e.balance=100n*UNIT;e.realised=-2200n*UNIT;e.note('UNIT_SYNTHETIC_LOSS_NOT_PERFORMANCE',{});
    const d=new Date(START);d.setHours(0,0,0,0);o.session.bridge.api.probeRequire('../safety.cjs').highWater(d.getTime(),-o.profile.launch.independentGuards.dayHighWaterDrawdownUsd-1);o.session.bridge.api.guardTick();
    assert.equal(o.session.bridge.api.cfg.live,false);drive(o,2500,{strong:true});o.flush(START+2500);
    assert.ok(o.stats.rawCandidates>0);assert.ok(o.history.totals().sim_decisions>0);assert.equal(e.counts.submitted,0);assert.equal(e.balance,100n*UNIT);assert.equal(e.realised,-2200n*UNIT);
    o.stop('UNIT_STOP_LATCHED',START+2500);o=new PersistentObserver({stateDir:x.dir,clock:()=>START+3000});
    assert.equal(o.session.engine.balance,100n*UNIT);assert.equal(o.session.engine.realised,-2200n*UNIT);assert.equal(o.session.bridge.api.cfg.live,false);
  }finally{cleanup(x,o);}
});
test('DB quota/disk failure freezes accounting/new entries and exposes loss while input processing continues',()=>{
  const x=temp();let o;try{
    o=new PersistentObserver({stateDir:x.dir,clock:()=>START});init(o);seedPending(o);const cash=o.session.engine.balance;
    o.history.checkSpace=()=>{throw new Error('SQLITE_FULL_UNIT_MOCK');};o.flush(START+1000);
    assert.ok(o.storage.dbBlocked);assert.ok(o.session.engine.accountingQuarantined);assert.equal(o.session.engine.balance,cash);assert.equal(o.session.engine.reserved,2100n*UNIT);
    const seq=o.continuity.cursor;o.batch(envelope([market(seq+1,START+1100,'hl')],START+1100),START+1100);
    assert.ok(o.stats.events>0);assert.equal(o.session.engine.submit({strategy:'book',side:'S',margin:2100,ctx:{}}),null);assert.ok(o.storage.historyLoss>0);
  }finally{cleanup(x,o);}
});
test('raw quota never removes protected unresolved context and clearly reports partial/raw loss',()=>{
  const x=temp();let h;try{
    h=new History(x.dir,{now:START});h.begin(buildProfile(),START);
    const p={...buildProfile().rawPolicy,quotaBytes:1},warnings=[];const raw=new RawRetention(h,p,{onWarning:x=>warnings.push(x)});
    raw.observe(market(1,START,'hl'),START,'boot');raw.trigger('unit-signal',START,{force:true});raw.flush(START);
    assert.equal(raw.disabled,false,'quota pressure drops the chunk and retries; it no longer kills the store');assert.ok(warnings.includes('RAW_QUOTA_PROTECTED_EVIDENCE'));assert.ok(raw.stats.storageLost>0);assert.equal(raw.warning,'RAW_QUOTA_PROTECTED_EVIDENCE');assert.equal(raw.status().coverage.includes('NOT a second full collector'),true);
  }finally{h?.close();fs.rmSync(x.parent,{recursive:true,force:true});}
});
test('raw protected segment with unknown intent survives retention and causes bounded quota warning instead of deletion',()=>{
  const x=temp();let o;try{
    o=new PersistentObserver({stateDir:x.dir,clock:()=>START});init(o);seedPending(o);o.raw.observe(market(1,START,'hl'),START,'boot');o.raw.trigger('unit',START,{force:true});o.raw.flush(START);
    const rows=o.history.db.prepare('SELECT * FROM raw_segments WHERE protected=1').all();assert.ok(rows.length);
    const before=rows.map(r=>path.join(x.dir,r.file));o.raw.policy={...o.raw.policy,quotaBytes:1,retentionMs:0};assert.equal(o.raw.prune(START+999999,1),false);
    for(const f of before)assert.ok(fs.existsSync(f));
  }finally{cleanup(x,o);}
});
test('R3 guard consumes same in-memory feed and cannot fetch/write; ALLOW_SHADOW is timing-only',async()=>{
  const x=temp();let o;try{
    const api=loadR3();assert.throws(()=>api.atomicWrite('/tmp/forbidden',{}),/DENIED/);
    await assert.rejects(api.fetchBatch(0),/DENIED/);
    o=new PersistentObserver({stateDir:x.dir,clock:()=>START});init(o);drive(o,3000,{strong:false});o.flush(START+3000);
    const s=o.guard.status(START+3000);assert.equal(s.live_permission,false);assert.equal(s.request_specific.rank,null);assert.equal(s.request_specific.status,'UNKNOWN_NO_MATCHED_REQUEST_FEED');
    assert.ok(o.history.totals().sim_forecasts>0);assert.equal(o.session.engine.counts.submitted,0);
  }finally{cleanup(x,o);}
});
test('single-owner and filesystem boundaries refuse production/private paths, symlinks and missing owned DB without reset',()=>{
  const x=temp();let h;try{
    assert.throws(()=>new History(path.join(ROOT,'apps/vchera2/.runtime/blocked')));
    h=new History(x.dir,{now:START});assert.throws(()=>new History(x.dir,{now:START}),/SINGLE_OWNER/);h.close();
    fs.unlinkSync(path.join(x.dir,DB_NAME));assert.throws(()=>new History(x.dir,{now:START}),/MISSING_NO_FREE_RESET/);
    const linked=path.join(x.parent,'linked');fs.symlinkSync(path.join(ROOT,'apps/vchera2'),linked);assert.throws(()=>new History(linked),/DENIED/);
  }finally{h?.close();fs.rmSync(x.parent,{recursive:true,force:true});}
});
test('readonly query/export returns bounded SIM summary without internal payload/SQL/network capability',()=>{
  const x=temp();let o;try{
    o=new PersistentObserver({stateDir:x.dir,clock:()=>START});init(o);drive(o,1000);o.flush(START+1000);
    const data=queryHistory(x.dir,{limit:20});assert.equal(data.financialPerformance,'SIM_NOT_CASH_PAYOUT');assert.equal(data.internalPayloadsIncluded,false);assert.equal(data.runs.length,1);assert.equal(data.segments.length,1);
    assert.equal(JSON.stringify(data).includes('payload_json'),false);assert.throws(()=>queryHistory(x.dir,{limit:1001}));assert.throws(()=>queryHistory(x.dir,{limit:'1; DROP TABLE sim_runs'}));
  }finally{cleanup(x,o);}
});
test('bounded child stop/status IPC, no listener leak and stable schema while running/stopped, with mocked transport only',async()=>{
  const x=temp();let service;try{
    let seq=0;const reader={get:async()=>{const t=Date.now();return envelope([market(++seq,t,'hl')],t);},close(){this.closed=true;}};
    service=new PersistentService({stateDir:x.dir,reader});const child=new EventEmitter();child.connected=true;child.messages=[];child.send=x=>child.messages.push(x);child.disconnect=()=>{child.connected=false;};
    service.attachLifecycle(child);await service.start();await new Promise(r=>setTimeout(r,180));child.emit('message',{type:'status',requestId:'one'});
    const snapshot=child.messages.find(x=>x.type==='shadow_status').status;assert.equal(snapshot.schemaVersion,1);assert.equal(snapshot.mode,'READ_ONLY_RESEARCH_NO_ORDERS');assert.equal(snapshot.financialMutations,0);assert.ok(snapshot.input);assert.ok(snapshot.assets);assert.ok(snapshot.acceptance);assert.equal(snapshot.fixedPreset.notionalUsd,1260000);
    const t=Date.now();child.emit('SIGTERM');const final=await service.done;assert.ok(Date.now()-t<2000);assert.equal(final.status,'STOPPED');assert.equal(final.service.completed,false);assert.equal(child.listenerCount('message'),0);assert.ok(reader.closed);
    assert.equal(JSON.parse(fs.readFileSync(path.join(x.dir,'telemetry.json'),'utf8')).status,'STOPPED');
  }finally{if(service&&!service.stopRequested)await service.stop();fs.rmSync(x.parent,{recursive:true,force:true});}
});
test('stop cancels an unresolved mocked request immediately and no other source URL can start',async()=>{
  const x=temp();let service;try{
    assert.throws(()=>new PersistentService({stateDir:x.dir,source:'http://localhost:18762/api/research/events'}),/DENIED/);
    service=new PersistentService({stateDir:x.dir,reader:{get:()=>new Promise(()=>{}),close(){}}});await service.start();const t=Date.now();await service.stop('UNIT_PENDING_GET');assert.ok(Date.now()-t<1000);assert.equal(service.status().status,'STOPPED');
  }finally{if(service&&!service.stopRequested)await service.stop();fs.rmSync(x.parent,{recursive:true,force:true});}
});
test('source queue/guard rejection log is durable once per source event across short restart, without changing strategy',()=>{
  const x=temp();let o;try{
    o=new PersistentObserver({stateDir:x.dir,clock:()=>START});init(o);
    o.session.bridge.api.events.push({t:START,kind:'skip',text:'UNIT_ROUND_GUARD_REJECTION_NOT_PERFORMANCE'});
    o.heartbeat(START);o.flush(START);
    const count=()=>o.history.db.prepare("SELECT COUNT(*) n FROM quality_events WHERE kind='SOURCE_MODEL_EVENT'").get().n;
    assert.equal(count(),1);o.stop('UNIT_SOURCE_LOG_STOP',START);
    o=new PersistentObserver({stateDir:x.dir,clock:()=>START+500});o.batch(envelope([],START+500,{last:0}),START+500);o.heartbeat(START+500);
    assert.equal(count(),1);assert.equal(o.session.engine.counts.submitted,0);assert.equal(o.session.engine.balance,2300n*UNIT);
  }finally{cleanup(x,o);}
});
