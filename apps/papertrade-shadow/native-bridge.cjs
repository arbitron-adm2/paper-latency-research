'use strict';
// Execute pinned source rules in a capability-restricted VM. Never call start().
const fs=require('node:fs'),path=require('node:path'),vm=require('node:vm'),crypto=require('node:crypto');
const ROOT=path.resolve(__dirname,'../..');
const digest=x=>crypto.createHash('sha256').update(x).digest('hex');
function createNativeBridge(engine,{asset,namespace,sourceRoot=path.join(__dirname,'reviewed-native')},db,start){
  if(!['BTC','ETH'].includes(asset)||!/^sim_native_[a-z0-9_]+$/.test(namespace))throw new Error('NATIVE_PROFILE_NOT_ALLOWED');
  const base=asset.toLowerCase()+'_lag',tableName=namespace+'_trades';
  const sql=text=>String(text).replace(new RegExp('\\b'+base+'_[a-z_]+\\b','g'),name=>namespace+name.slice(base.length));
  const privateDb={exec:text=>db.exec(sql(text)),prepare:text=>db.prepare(sql(text))};
  let now=start;const deny=()=>{throw new Error('NATIVE_SIM_CAPABILITY_DENIED');};
  class Clock extends Date{constructor(...a){super(...(a.length?a:[now]));}static now(){return now;}}
  const account={wallet:{t:start,available:2300,balance:2300,connected:true,openPositions:[],operations:[]},
    keyStatus:()=>({active:true}),onTrade:()=>{},onQueueSample:()=>{},queueSamples:()=>[],confirmClosed:()=>{},
    status:()=>({key:{active:true},wallet:{},tradesConnected:true}),open:deny,close:deny,cancel:id=>{engine.cancel(id,'source cancel');return Promise.resolve({virtual:true,terminal:false});}};
  const cache=new Map(),manifest=JSON.parse(fs.readFileSync(path.join(sourceRoot,'manifest.json'),'utf8'));
  function load(name){
    if(cache.has(name))return cache.get(name);
    if(![asset.toLowerCase()+'-lag.js','funding-guard.js','regime.js'].includes(name))return deny();
    let source=fs.readFileSync(path.join(sourceRoot,name),'utf8');
    if(digest(source)!==manifest.files[name])throw new Error('NATIVE_SOURCE_PIN_CHANGED:'+name);
    const module={exports:{}};
    const sandbox={module,exports:module.exports,Date:Clock,
      console:Object.freeze({log:()=>{},error:()=>{}}),__dirname:'/virtual',__filename:name,__execution:engine,
      setInterval:deny,clearInterval:deny,setTimeout:deny,clearTimeout:deny,fetch:deny,
      WebSocket:class{constructor(){deny();}}};
    sandbox.globalThis=sandbox;sandbox.global=sandbox;
    sandbox.require=id=>{
      if(id==='./../db')return{db:privateDb};
      if(id==='fs')return Object.freeze({existsSync:()=>false,readFileSync:deny,writeFileSync:deny,appendFileSync:deny});
      if(id==='path')return path;
      if(id==='./papertrade')return new Proxy(account,{get:(o,k)=>k in o?o[k]:deny});
      if(id==='./funding-guard')return load('funding-guard.js');
      if(id==='./regime')return load('regime.js');
      if(id==='./queue-stream.cjs')return Object.freeze({start:()=>{},getState:()=>({available:false,source:'SHARED_FEED_ONLY'})});
      return deny();
    };
    if(name.endsWith('-lag.js'))source+=`
      // Only SIM sizing and disabled external integrations differ from source defaults.
      cfg={...cfg,live:true,enabled:true,margin:2100,leverage:600,maxPositions:1,addMargin:2100,
        sizeToOi:false,splitParts:1,tgEnabled:false,tgTrades:false,refEnabled:false,autoTune:false,autoProfile:false,schedEnabled:false};
      if(typeof WARMUP_MS!=='undefined')WARMUP_MS=180000;
      const __nativeDecide=decideEntry;
      decideEntry=function(strategy,sig,side){const ok=__nativeDecide(strategy,sig,side);__execution.recordDecision(strategy,side,sig,ok,lastDecision[strategy]);return ok;};
      liveEntry=function(strategy,side,ctx,margin=cfg.margin){
        const q={strategy,side,ctx,sig:ctx.sig,margin,createdAt:Date.now(),px:mid('hl'),fillAt:Infinity,live:true,intentId:null,rank:null};
        q.intentId=__execution.submit(q);if(q.intentId===null)return null;pending.push(q);return q;
      };
      sendClose=async function(p){if(!p.closing||p.closing.sent)return;p.closing.intentId=__execution.close(p);p.closing.sent=Date.now();};
      module.exports.simBridge={feed,pending,positions,closePx,events,lastDecision,guard,tick,guardTick,maxHlSpread:HL_MAX_SPREAD_USD,
        get cfg(){return cfg;},
        regimeTick:()=>{if(typeof regimeTick==='function')regimeTick(volNow());},
        signalState:()=>({bookSide:bookSig.side,bookSince:bookSig.since,lastDecision}),
        btcQuote:(bid,ask,t)=>{if(typeof feedBtc!=='undefined')Object.assign(feedBtc,{bid,ask,t});},
        resetInput(){sBuf.length=0;bfBuf.length=0;mbuf.length=0;baseline=NaN;bookSig={side:null,since:Date.now()};
          for(const f of Object.values(feed)){f.t=0;f.bookT=0;f.bids=[];f.asks=[];f.bid=NaN;f.ask=NaN;}},
        fill(q,x){const i=pending.indexOf(q);if(i>=0)pending.splice(i,1);return openPosition(q.strategy,q.side,{...q.ctx,fill:{...x,decidedAt:q.createdAt,decidedPx:q.px}});},
        terminal(q){const i=pending.indexOf(q);if(i>=0)pending.splice(i,1);},
        closePosition,
        state:()=>({cfg,guard:{...guard},lastClosedAt:{...lastClosedAt},lastEntry:{...lastEntry},positions:[...positions.values()]}),
        restoreFlat(s){if(!s)return;cfg={...cfg,...s.cfg};Object.assign(guard,s.guard);Object.assign(lastClosedAt,s.lastClosedAt);if(s.lastEntry)Object.assign(lastEntry,s.lastEntry);},
        rawCandidates(){const books=V.map(bookStat),sp=mid('hl')-mid('bf'),dev=sp-baseline,out=[];
          if(cfg.spreadEnabled&&Number.isFinite(dev)&&Math.abs(dev)>=cfg.minDev&&Math.abs(dev)<=cfg.maxDev)out.push({strategy:'spread',side:dev>0?'S':'L',signal:Math.abs(dev)});
          if(cfg.bookEnabled&&bookSig.side&&Date.now()-bookSig.since>=cfg.bookConfirmMs)out.push({strategy:'book',side:bookSig.side,signal:bookStrength(books)});return out;}
      };`;
    vm.runInNewContext(source,sandbox,{filename:'SIM:'+name,timeout:2000,contextCodeGeneration:{strings:false,wasm:false}});
    cache.set(name,module.exports);return module.exports;
  }
  const api=load(asset.toLowerCase()+'-lag.js').simBridge;
  let lastGuard=0;
  function setTime(t){now=t;Object.assign(account.wallet,{t,available:Number(engine.available())/1e18,balance:Number(engine.balance)/1e18});}
  function market(e){
    engine.executionPrices?.observe(e);
    if(e.symbol==='BTC'&&asset==='ETH'&&e.venue==='bf'&&e.payload.frameKind==='quote')api.btcQuote(Number(e.payload.bid),Number(e.payload.ask),Math.min(e.recv_ms,e.source_ms??e.recv_ms));
    if(e.symbol!==asset)return;
    const f=api.feed[e.venue],p=e.payload;if(!f)return;
    const t=Math.min(e.recv_ms,e.source_ms??e.recv_ms);
    if(p.frameKind==='book'){
      if(t<(f.bookT??0))return;f.bids=p.bids.map(x=>x.map(Number));f.asks=p.asks.map(x=>x.map(Number));f.bookT=t;
    }else if(t>=(f.t??0)){f.bid=Number(p.bid);f.ask=Number(p.ask);f.t=t;if(e.venue==='hl'&&f.ask-f.bid<=api.maxHlSpread){f.midOk=(f.bid+f.ask)/2;f.midOkT=t;}}
  }
  function step(t){
    setTime(t);const f=api.feed;
    const fresh=['hl','bf'].every(v=>f[v].t>0&&t-f[v].t<=api.cfg.maxFeedAgeMs);
    const depth=['hl','bf'].every(v=>f[v].bids.length&&f[v].asks.length&&t-f[v].bookT<=(v==='hl'?10000:3000));
    const allowed=engine.inputHealthy!==false&&fresh&&depth&&t>=engine.integrityWarmUntil;
    const enabled=api.cfg.enabled;if(!allowed)api.cfg.enabled=false;
    try{api.tick();}finally{if(!allowed)api.cfg.enabled=enabled;}
    if(t-lastGuard>=1000){api.regimeTick();api.guardTick();lastGuard=t;}
    for(const q of [...api.pending])if((q.signalGone||q.missed||t-q.createdAt>api.cfg.entryMaxWaitMs)&&!q.cancelRequestedAt){q.cancelRequestedAt=t;q.cancelReason=q.signalGone?'source signal gone':q.missed?'source missed move':'source entry timeout';engine.cancel(q.intentId,q.cancelReason);}
  }
  return{api,db,tableName,maxFillSpread:api.maxHlSpread,market,step,setTime,invalidate(t,reason){api.resetInput();engine.inputHealthy=false;engine.integrityWarmUntil=t+180000;},
    close(){},sourceManifest:manifest,checkpointState:()=>api.state(),restoreState:s=>api.restoreFlat(s)};
}
module.exports={createNativeBridge};
