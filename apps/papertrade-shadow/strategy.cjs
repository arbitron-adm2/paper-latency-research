'use strict';
const fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const {DatabaseSync}=require('node:sqlite');
const crypto=require('node:crypto');
const ROOT=path.resolve(__dirname,'../..');
// Explicit immutable research profile. Missing/changed snapshot fails closed; there is no fallback to current LIVE code.
const SOURCE_ROOT=path.join(__dirname,'reviewed-baseline');
const SOURCES=['scanner/btcy2-lag.js','safety.cjs','execution-state.cjs','execution-journal.cjs','trading-controls.cjs','paper-quote.cjs','scanner/regime.js','scanner/funding-guard.js','market-data.cjs'];
const sha=b=>crypto.createHash('sha256').update(b).digest('hex');
function sourceManifest(){return SOURCES.map(p=>({path:'apps/vchera2/'+p,sha256:sha(fs.readFileSync(path.join(SOURCE_ROOT,'apps/vchera2',p)))}));}
function createBridge(engine, launch, experiment, start) {
  if(engine.persistentDb&&!engine.config.persistentService?.enabled)throw new Error('SIM_DB_DI_REQUIRES_PERSISTENT_PROFILE');
  const ownsDb=!engine.persistentDb,db=engine.persistentDb??new DatabaseSync(':memory:');
  let now=start;
  class Clock extends Date { constructor(...args){super(...(args.length?args:[now]));} static now(){return now;} }
  const deny=()=>{throw new Error('SHADOW_CAPABILITY_DENIED');};
  const memory=new Map(), cache=new Map();
  const config=structuredClone(launch);
  const window=engine.config.researchWindow;
  const persistent=engine.config.persistentService?.enabled===true;
  if(persistent&&(engine.config.persistentService.acknowledgeSimulationOnly!==true||engine.config.persistentService.cutoffPolicy!=='continuous-observer-only-no-live'))throw new Error('PERSISTENT_SIM_POLICY_REQUIRED');
  if(window&&(!['respect-production','research-window'].includes(window.cutoffPolicy)||!Number.isSafeInteger(window.startMs)||!Number.isSafeInteger(window.endMs)||window.endMs<=window.startMs||window.endMs-window.startMs>930000||window.cutoffPolicy==='research-window'&&window.acknowledgeResearchOnlyOverride!==true))throw new Error('Explicit bounded research cutoff acknowledgement required');
  const baselineCutoff=Date.parse(launch.independentGuards.tradeUntilAbsolute);
  const effectiveCutoff=persistent?Date.parse('9999-12-31T23:59:59.999Z'):(window?.cutoffPolicy==='research-window'?window.endMs:baselineCutoff);
  // Explicit research-only cutoff DI must happen BEFORE safety.cjs closes over G:
  // lockedStrategyConfig, tradingWindowReason and cutoffReached share this clone.
  // Original launch/source/default lock and all exposure/independent risk guards stay unchanged.
  if(persistent||window?.cutoffPolicy==='research-window')config.independentGuards.tradeUntilAbsolute=new Date(effectiveCutoff).toISOString();
  const sandboxBase={Date:Clock,Math:Object.freeze(Object.assign(Object.create(Math),{random:deny})),
    console:{log:()=>{},error:()=>{}},setInterval:deny,clearInterval:deny,setTimeout:deny,clearTimeout:deny,
    fetch:deny,WebSocket:class{constructor(){deny();}},AbortSignal:undefined};
  function load(name) {
    if(cache.has(name))return cache.get(name);
    if(!SOURCES.includes(name))return deny();
    const module={exports:{}};
    const context={...sandboxBase,module,exports:module.exports,__dirname:'/virtual',__filename:name};
    context.require=id=>{
      const map={'./execution-state.cjs':'execution-state.cjs','../execution-state.cjs':'execution-state.cjs',
        '../execution-journal.cjs':'execution-journal.cjs','../trading-controls.cjs':'trading-controls.cjs',
        '../paper-quote.cjs':'paper-quote.cjs','./regime':'scanner/regime.js','./funding-guard':'scanner/funding-guard.js'};
      if(id==='node:fs'||id==='fs')return Object.freeze({readFileSync:p=>{
        if(!memory.has(p)){const e=new Error('ENOENT virtual');e.code='ENOENT';throw e;}return memory.get(p);
      },existsSync:()=>false,appendFileSync:deny,writeFileSync:deny});
      if(id==='node:path'||id==='path')return path;
      if(id==='./launch-config.json')return config;
      if(id==='./runtime.cjs')return {privateRuntime:()=>'/virtual',atomicPrivateJSON:(p,x)=>memory.set(p,JSON.stringify(x))};
      if(id==='./../db')return {db};
      if(id==='../safety.cjs') {
        const s=load('safety.cjs');
        return {...s,cutoffReached:(t=now)=>t>=effectiveCutoff,
          tradingWindowReason:(t=now)=>window?.cutoffPolicy==='research-window'?(t>=effectiveCutoff?'explicit research window ended at '+new Date(effectiveCutoff).toISOString():null):s.tradingWindowReason(t),
          lockedStrategyConfig:defaults=>{
          const original=s.lockedStrategyConfig(defaults);
          return {...original,margin:experiment.margin,leverage:experiment.leverage};
        }};
      }
      if(id==='./papertrade')return {getAccount:()=>account};
      // New production dependency is used only by connectFeeds(), which is never run here.
      // Canonical tape still enters via market(); no production singleton is instantiated.
      if(id==='../market-data.cjs')return Object.freeze({marketData:Object.freeze({binance:deny,hyperliquid:deny,snapshot:deny})});
      if(map[id])return load(map[id]);
      return deny();
    };
    context.global=context;
    let source=fs.readFileSync(path.join(SOURCE_ROOT,'apps/vchera2',name),'utf8');
    if(name==='scanner/regime.js'){
      const marker='return { update, get regime()';
      if(source.split(marker).length!==2)throw new Error('REVIEWED_REGIME_CHECKPOINT_SHAPE_CHANGED');
      source=source.replace(marker,'return { __observerRestore:x=>Object.assign(st,x), update, get regime()');
    }
    if(name==='safety.cjs')source+='\nmodule.exports.__observerState=()=>({hwState,walls:{until:walls.until,last:walls.last}}); module.exports.__observerRestore=x=>{hwState=x.hwState;walls.until=x.walls.until;walls.last=x.walls.last;};';
    if(name==='scanner/btcy2-lag.js')source+=`\n// Harness-only dependency injection; original files are not edited.\n
      WARMUP_MS=180000;
      const __originalDecideEntry=decideEntry;
      decideEntry=function(strategy,sig,side){const ok=__originalDecideEntry(strategy,sig,side);
        if(__execution.recordDecision)__execution.recordDecision(strategy,side,sig,ok,lastDecision[strategy]);return ok;};
      liveEntry=function(strategy,side,ctx,margin=cfg.margin){
        const q={strategy,side,ctx,sig:ctx.sig,margin,createdAt:Date.now(),px:mid('hl'),live:true,intentId:null,rank:null};
        q.intentId=__execution.submit(q);if(q.intentId==null)return null;
        const canonical=__execution.intents.get(q.intentId)?.q||q;canonical.intentId=q.intentId;
        if(!pending.some(x=>x.intentId===q.intentId))pending.push(canonical);return canonical;
      };
      sendClose=async function(p){if(!p.closing||p.closing.sent)return;
        p.closing.intentId=__execution.close(p);p.closing.sent=Date.now();};
      module.exports.bridge={feed,pending,positions,tick,guardTick,regimeTick,cfg,oi,executionSync,
        openPosition,closePosition,closePx,events,lastDecision,
        signalState:()=>({bookSide:bookSig.side,bookSince:bookSig.since,lastDecision,guard:{...guard}}),
        restoreFlatPolicy:s=>{Object.assign(cfg,s.cfg);Object.assign(guard,s.guard);},
        observeCandidates(){const books=V.map(bookStat),sp=mid('hl')-mid('bf'),dev=sp-baseline,out=[];
          if(cfg.spreadEnabled&&Number.isFinite(dev)&&Math.abs(dev)>=cfg.minDev&&Math.abs(dev)<=cfg.maxDev)out.push({strategy:'spread',side:dev>0?'S':'L',signal:Math.abs(dev)});
          if(cfg.bookEnabled&&bookSig.side&&Date.now()-bookSig.since>=cfg.bookConfirmMs)out.push({strategy:'book',side:bookSig.side,signal:bookStrength(books)});
          return out.map(x=>({...x,atMs:Date.now(),kind:'RAW_TRIGGER_NOT_FULL_BASELINE_PERMISSION',sourceEnabled:!!cfg.enabled,sourceLiveBranch:!!cfg.live,sourceGuard:{...guard},sourceDecision:lastDecision[x.strategy]}));},
        diagnosticSnapshot(strategy,side){const now=Date.now(),nativeBooks=V.map(bookStat),books=V.map((venue,i)=>({venue,...nativeBooks[i],quoteAgeMs:age(venue),bookAgeMs:feed[venue].bookT?now-feed[venue].bookT:null}));
          return {atMs:now,strategy,side,prices:Object.fromEntries(V.map(v=>[v,Number.isFinite(mid(v))?mid(v):null])),
            spreadUsd:Number.isFinite(mid('hl')-mid('bf'))?mid('hl')-mid('bf'):null,baselineUsd:Number.isFinite(baseline)?baseline:null,baselineSlowUsd:Number.isFinite(baselineSlow)?baselineSlow:null,
            deviationUsd:Number.isFinite(mid('hl')-mid('bf')-baseline)?mid('hl')-mid('bf')-baseline:null,books,bookImbalanceUnit:'fraction -1..1; multiply100 for percent',
            bookConfirmation:{side:bookSig.side,heldMs:bookSig.side?now-bookSig.since:0,requiredMs:cfg.bookConfirmMs},
            binanceMoveUsd:side?(side==='L'?1:-1)*(mid('bf')-bfAgo(cfg.binMoveSec)):null,binanceRange10sUsd:(()=>{const p=mbuf.filter(x=>x[0]>=now-10000).map(x=>x[2]);return p.length?Math.max(...p)-Math.min(...p):null;})(),directionalBookFavor:side?bookFavor(nativeBooks,side):null,
            thresholds:Object.fromEntries(['minDev','maxDev','minSamples','minSig','bookMinImb','bookMinSizeUsd','bookConfirmMs','cooldownSec','bookFilterImb','binMoveSec','minBinMove','vetoAgainstUsd','g3Enabled','g3MaxBinMove','g3MaxCushion','g3MinBfBook','maxBinRange10s','cushBoth','slowBaselineMin','maxFeedAgeMs','noLongs','wallGuard','regHaltHigh','regLowUsd','regHighUsd'].map(k=>[k,cfg[k]??null])),
            state:{positions:positions.size,pending:pending.length,pausedUntil:guard.pausedUntil,dayPnl:guard.dayPnl,lastEntry:{...lastEntry}},lastSourceDecision:lastDecision[strategy]?{...lastDecision[strategy]}:null};},
        bindPending(intents){const rows=[...intents.values()].filter(r=>r.kind==='entry'&&['submitted','queued','outcome_unknown'].includes(r.state));pending.splice(0,pending.length,...rows.map(r=>r.q));},
        observerState(){return {startedAt:STARTED_AT,cfg,feed,pending,positions,closePx,events,sBuf,bfBuf,mbuf,chart,skippedLongs,myOpens,
          objects:{lastEntry,oiSize,split,throttled,executionSync,lastDecision,skipLogged,paperCancels,guard,oiReject,oiRejects},maps:{cancelledLive,orphans},
          vars:{paperMarketQuote,baseline,lastBaseline,lastChart,lastSnap,baselineSlow,lastBaselineSlow,baselineCx,lastBaselineCx,ethWant,liveBusy,liveBusyAt,liveStage,evCache,regR5m,bookSig,summaryAt,refLast,tuneLast,oi,oiEtag,oiBusy,paperDelayCache,exitByHoldCache,qPool,qPoolAt,skippedSeq,skippedLastAt,mdumpT},
          regime:regime?{opts:regime.opts,state:regime.state()}:null};},
        observerRestore(s){if(s.startedAt!==STARTED_AT)throw new Error('VM_START_ORIGIN_MISMATCH');cfg=s.cfg;
          Object.assign(feed,s.feed);for(const [target,key] of [[pending,'pending'],[events,'events'],[sBuf,'sBuf'],[bfBuf,'bfBuf'],[mbuf,'mbuf'],[chart,'chart'],[skippedLongs,'skippedLongs'],[myOpens,'myOpens']])target.splice(0,target.length,...s[key]);
          positions.clear();for(const [k,v] of s.positions)positions.set(k,v);closePx.clear();for(const [k,v] of s.closePx)closePx.set(k,v);
          for(const [name,obj] of Object.entries({lastEntry,oiSize,split,throttled,executionSync,lastDecision,skipLogged,paperCancels,guard,oiReject,oiRejects})){for(const k of Object.keys(obj))delete obj[k];Object.assign(obj,s.objects[name]);}
          cancelledLive.clear();for(const [k,v] of s.maps.cancelledLive)cancelledLive.set(k,v);orphans.clear();for(const [k,v] of s.maps.orphans)orphans.set(k,v);
          const v=s.vars;paperMarketQuote=v.paperMarketQuote;baseline=v.baseline;lastBaseline=v.lastBaseline;lastChart=v.lastChart;lastSnap=v.lastSnap;baselineSlow=v.baselineSlow;lastBaselineSlow=v.lastBaselineSlow;baselineCx=v.baselineCx;lastBaselineCx=v.lastBaselineCx;ethWant=v.ethWant;liveBusy=v.liveBusy;liveBusyAt=v.liveBusyAt;liveStage=v.liveStage;evCache=v.evCache;regR5m=v.regR5m;bookSig=v.bookSig;summaryAt=v.summaryAt;refLast=v.refLast;tuneLast=v.tuneLast;oi=v.oi;oiEtag=v.oiEtag;oiBusy=v.oiBusy;paperDelayCache=v.paperDelayCache;exitByHoldCache=v.exitByHoldCache;qPool=v.qPool;qPoolAt=v.qPoolAt;skippedSeq=v.skippedSeq;skippedLastAt=v.skippedLastAt;mdumpT=v.mdumpT;
          regime=s.regime?regimeMod.createRegime(s.regime.opts):null;if(regime)regime.__observerRestore(s.regime.state);
        },
        resetInput(t){sBuf.length=0;bfBuf.length=0;mbuf.length=0;baseline=NaN;bookSig={side:null,since:t};
          for(const f of Object.values(feed)){f.t=0;f.bookT=0;f.sourceTime=0;f.quoteSourceTime=0;f.bookSourceTime=0;f.bid=NaN;f.ask=NaN;f.bids=[];f.asks=[];f.midOk=NaN;f.midOkT=0;}},
        fill(q,x){const i=pending.indexOf(q);if(i>=0)pending.splice(i,1);
          return openPosition(q.strategy,q.side,{...q.ctx,fill:{...x,decidedAt:q.createdAt,decidedPx:q.px,
            invalidatedReason:q.cancelReason||null,signalGone:!!q.signalGone,cancelFailed:q.cancelFailed||null}});},
        terminal(q){const i=pending.indexOf(q);if(i>=0)pending.splice(i,1);},
        probeRequire:require,probeFetch:()=>fetch('https://example.org'),probeSigner:()=>pt.ensureKey()};`;
    if(name==='scanner/btcy2-lag.js')source+='\nObject.defineProperty(module.exports.bridge,"cfg",{get:()=>cfg});';
    context.__execution=engine;
    vm.runInNewContext(source,context,{filename:name,timeout:2000,contextCodeGeneration:{strings:false,wasm:false}});
    cache.set(name,module.exports);
    return module.exports;
  }
  const account=new Proxy({
    wallet:{t:start,available:Number(engine.available())/1e18,openPositions:[],operations:[],connected:true},
    keyStatus:()=>({active:true,master:launch.wallet}),onTrade:()=>{},onQueueSample:()=>{},queueSamples:()=>[],
    confirmClosed:()=>{},status:()=>({key:{active:true},wallet:{},tradesConnected:true}),
    open:deny,close:deny,cancel:id=>{engine.cancel(id,'source guard requested cancel');return Promise.resolve({virtual:true,terminal:false});},
  },{get(target,key){if(key in target)return target[key];return deny;}});
  const api=load('scanner/btcy2-lag.js').bridge;
  function setTime(t){now=t;account.wallet.t=t;account.wallet.available=Number(engine.available())/1e18;
    api.executionSync.ready=true;api.executionSync.t=t;}
  function market(e){
    engine.executionPrices?.observe(e);
    const f=api.feed[e.venue];if(!f)throw new Error('Unsupported venue');
    const p=e.payload;
    const sourceTime=Number(e.event_ms), receive=Number(e.recv_ms);
    if(sourceTime>receive+1000){engine.note('clock_skew',{source:e.source,ms:sourceTime-receive});return;}
    if(!(Number(p.bid)>0&&Number(p.ask)>=Number(p.bid))){engine.note('invalid_quote',{});return;}
    const isBook=!!(p.bids&&p.asks),lastQuote=f.quoteSourceTime??f.sourceTime??0,lastBook=f.bookSourceTime??0;
    if((isBook&&sourceTime<lastBook)||(!isBook&&sourceTime<lastQuote)){
      engine.note('out_of_order_ignored',{source:e.source,frame:isBook?'book':'quote',reason:'same-channel source clock older; receive sequence is not a gap'});return true;
    }
    // Frozen native callbacks: BBO owns quote fields; depth owns only book fields.
    // Even a newer book must not renew quote prices, age or HL midOk.
    if(!isBook&&sourceTime>=lastQuote){
      f.bid=Number(p.bid);f.ask=Number(p.ask);f.t=Math.min(sourceTime,receive);f.sourceTime=sourceTime;f.quoteSourceTime=sourceTime;
      if(e.venue==='hl'&&f.ask-f.bid<=6){f.midOk=(f.bid+f.ask)/2;f.midOkT=f.t;}
    }
    if(isBook){
      if(!p.bids.length||!p.asks.length){f.bids=[];f.asks=[];f.bookT=0;return;}
      f.bids=p.bids.map(x=>x.map(Number));f.asks=p.asks.map(x=>x.map(Number));f.bookT=Math.min(sourceTime,receive);f.bookSourceTime=sourceTime;
    }
    if(e.quality?.gap||e.quality?.reconnect){f.bids=[];f.asks=[];f.bookT=0;engine.note('feed_gap',{venue:e.venue});}
    return true;
  }
  const originalEnabled=api.cfg.enabled;
  let lastBookSide=null,lastGuardSecond=null;
  function step(t){
    setTime(t);
    const f=api.feed,required=['hl','bf'];
    const fullDepth=required.every(v=>f[v].bids.length&&f[v].asks.length&&t-f[v].bookT<=(v==='hl'?10000:3000));
    const fresh=required.every(v=>Number.isFinite(f[v].bid)&&Number.isFinite(f[v].ask)&&t-f[v].t<=api.cfg.maxFeedAgeMs);
    const flags={warmup:t-start<180000,missingDepth:!fullDepth,stale:!fresh,
      integrity:engine.inputHealthy===false,recoveryWarmup:t<(engine.integrityWarmUntil??0),
      cutoff:t>=effectiveCutoff,outsideResearchWindow:!!window&&(t<window.startMs||t>=window.endMs),sourceDisabled:!api.cfg.live||!api.cfg.enabled};
    engine.lastInputChecks={atMs:t,...flags,fullDepth,fresh};
    engine.tickStats??={ticks:0,reasons:{},rawSignalTicks:0,signalTransitions:0,candidateEvaluations:0,approved:0,guardReasons:{}};
    engine.tickStats.ticks++;
    for(const [key,value] of Object.entries(flags))if(value)engine.tickStats.reasons[key]=(engine.tickStats.reasons[key]||0)+1;
    if(!fullDepth)engine.missingDepthTicks++;
    const enabled=api.cfg.enabled;
    const suppress=!fullDepth||!fresh||flags.integrity||flags.recoveryWarmup||flags.cutoff||flags.outsideResearchWindow||flags.sourceDisabled;
    if(suppress)api.cfg.enabled=false;
    api.tick();
    if(suppress)api.cfg.enabled=enabled&&originalEnabled;
    const signal=api.signalState();
    if(signal.bookSide)engine.tickStats.rawSignalTicks++;
    if(signal.bookSide&&signal.bookSide!==lastBookSide)engine.tickStats.signalTransitions++;
    if(!signal.bookSide)engine.tickStats.noBookSignalTicks=(engine.tickStats.noBookSignalTicks||0)+1;
    if(!flags.warmup)engine.tickStats.postWarmupTicks=(engine.tickStats.postWarmupTicks||0)+1;
    if(!suppress&&!flags.warmup)engine.tickStats.entryEligibleInputTicks=(engine.tickStats.entryEligibleInputTicks||0)+1;
    for(const decision of Object.values(signal.lastDecision))if(decision?.t===t&&decision.ok===false){
      const reason=String(decision.why||'unknown').replace(/\d+([.,]\d+)?/g,'#');
      engine.tickStats.guardReasons[reason]=(engine.tickStats.guardReasons[reason]||0)+1;
    }
    lastBookSide=signal.bookSide;
    if(Math.floor(t/1000)!==lastGuardSecond){lastGuardSecond=Math.floor(t/1000);api.regimeTick();api.guardTick();}
    for(const q of [...api.pending]){
      const execution=engine.intents.get(q.intentId);
      if(execution?.entryLimit&&t>execution.entryLimit.signalValidUntilMs&&!q.localSignalExpired){
        q.localSignalExpired=true;engine.counts.localSignalExpired++;engine.note('local_signal_expired',{intentId:q.intentId,risk_slot:'retained; expiry is not cancellation'});
      }
      const gone=q.signalGone,missed=q.missed,timed=t-q.createdAt>api.cfg.entryMaxWaitMs;
      if((gone||missed||timed)&&!q.cancelRequestedAt){
        q.cancelReason=gone?'сигнал пропал в очереди':missed?'HL ушёл без нас':'таймаут входа';
        q.cancelRequestedAt=t;engine.cancel(q.intentId,q.cancelReason);
      }
    }
  }
  let coalescedInvalidations=0;
  function invalidate(t,reason,{quiet=false}={}){api.resetInput(t);engine.inputHealthy=false;engine.integrityWarmUntil=t+180000;
    if(quiet){coalescedInvalidations++;return;} // same reset/warmup; only the repeated journal note is coalesced
    engine.note('input_invalidated',{reason,recoveryWarmUntil:engine.integrityWarmUntil,...(coalescedInvalidations?{coalescedRepeats:coalescedInvalidations}:{})});coalescedInvalidations=0;}
  function checkpointState(){
    let tables=null;
    if(ownsDb){tables={};for(const r of db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'btcy2_%'").all())tables[r.name]=db.prepare('SELECT * FROM "'+r.name+'"').all();}
    const nativeEpoch=ownsDb?null:Number(db.prepare("SELECT value FROM sim_meta WHERE key='native_epoch'").get()?.value??0);
    return {schemaVersion:1,now,start,lastBookSide,lastGuardSecond,memory:new Map(memory),safety:load('safety.cjs').__observerState(),native:api.observerState(),tables,nativeEpoch,nativeJournal:ownsDb?'MEMORY':'PRIVATE_SIM_HISTORY_NOT_REAL_VCHERA2_DB'};
  }
  function restoreState(s){
    if(s.schemaVersion!==1||s.start!==start)throw new Error('VM_CHECKPOINT_ORIGIN');
    if(!ownsDb&&s.nativeEpoch!==Number(db.prepare("SELECT value FROM sim_meta WHERE key='native_epoch'").get()?.value??0))throw new Error('VM_NATIVE_JOURNAL_CHANGED_AFTER_CHECKPOINT');
    const allowed=new Set(db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'btcy2_%'").all().map(r=>r.name));
    if(ownsDb)db.exec('BEGIN');try{for(const [name,rows] of Object.entries(s.tables??{})){
      if(!allowed.has(name))throw new Error('VM_TABLE_NOT_REVIEWED');db.exec('DELETE FROM "'+name+'"');
      for(const row of rows){const keys=Object.keys(row);if(keys.some(k=>!/^\w+$/.test(k)))throw new Error('VM_COLUMN');db.prepare('INSERT INTO "'+name+'" ('+keys.map(k=>'"'+k+'"').join(',')+') VALUES ('+keys.map(()=>'?').join(',')+')').run(...keys.map(k=>row[k]));}
    }if(ownsDb)db.exec('COMMIT');}catch(e){if(ownsDb)db.exec('ROLLBACK');throw e;}
    memory.clear();for(const [k,v] of s.memory)memory.set(k,v);load('safety.cjs').__observerRestore(s.safety);
    api.observerRestore(s.native);lastBookSide=s.lastBookSide;lastGuardSecond=s.lastGuardSecond;setTime(s.now);
  }
  function compactNative(t){db.prepare("DELETE FROM btcy2_lag_trades WHERE status='closed' AND opened_at<? AND id<(SELECT COALESCE(MAX(id),0)-3000 FROM btcy2_lag_trades)").run(t-7*86400000);}
  return {api,db,market,step,setTime,invalidate,checkpointState,restoreState,compactNative,marketPolicy:Object.freeze({id:'frozen-native-bbo-only-r2',depthDerivedQuote:false,quoteFields:'quote frames only',bookFields:'book frames only',ageClock:'min(source_ms,recv_ms); unchanged research policy, not native wall-time bit parity'}),cutoff:{policy:persistent?'continuous-observer-only-no-live':(window?.cutoffPolicy??'respect-production'),baselineCutoffMs:baselineCutoff,effectiveCutoffMs:effectiveCutoff,productionConfigModified:false},sourceManifest:sourceManifest(),close:()=>{if(ownsDb)db.close();}};
}
module.exports={createBridge,sourceManifest,ROOT,SOURCE_ROOT,sha};
