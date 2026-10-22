'use strict';
// Offline, independent accounts. No production client, keys or network access.
const fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto'),Module=require('node:module');
const load=Module._load,denied=new Set(['net','http','https','http2','tls','dns','dgram','undici','ws','ethers']);
Module._load=function(id,...args){if(denied.has(id.replace(/^node:/,'')))throw new Error('OFFLINE_NETWORK_DENIED:'+id);return load.call(this,id,...args);};
globalThis.fetch=()=>{throw new Error('OFFLINE_NETWORK_DENIED');};
const ROOT=path.resolve(__dirname,'../..'),C=require('../papertrade-shadow/state-codec.cjs');
const {SummaryBooks}=require('./summary-books.cjs'),{createSummaryBridgeFactory}=require('./summary-bridge.cjs');
const strategy=require('../papertrade-shadow/strategy.cjs'),originalCreate=strategy.createBridge,summaryFactory=createSummaryBridgeFactory();
strategy.createBridge=function(engine,...args){
 if(!engine.config.simProfile.id.startsWith('proxy_'))return originalCreate(engine,...args);
 engine.researchBooks=new SummaryBooks({policy:'bf_l1_proxy'});
 const bridge=summaryFactory.createBridge(engine,...args);engine.researchBooks.feed=bridge.api.feed;return bridge;
};
const {Replay}=require('./replay.cjs'),{installObservation}=require('../live-parity-backtest/strategy-replay.cjs'),{hasRisk}=require('../papertrade-shadow/engine-state.cjs'),{installMarkCache}=require('./fast-mark.cjs');
const profile=JSON.parse(fs.readFileSync(path.join(ROOT,'apps/vchera2/y2-live-profile.json')));
function options(){const r={};for(let i=2;i<process.argv.length;i+=2){const k=process.argv[i],v=process.argv[i+1];if(!['--from-ms','--to-ms','--warmup-ms','--out','--event-limit'].includes(k)||v==null)throw new Error('INVALID_ARGUMENT');r[k.slice(2)]=v;}return r;}
async function main(){
 const o=options(),start=Number(o['from-ms']),end=Number(o['to-ms']),warmup=Number(o['warmup-ms']??3600000),OUT=path.resolve(o.out),limit=Number(o['event-limit']??Infinity);
 if(!Number.isSafeInteger(start)||!Number.isSafeInteger(end)||start>=end||!Number.isSafeInteger(warmup)||warmup<180000||warmup>7200000)throw new Error('INVALID_WINDOW');
 fs.mkdirSync(OUT,{recursive:true});if(fs.existsSync(path.join(OUT,'result.json')))throw new Error('REFUSE_OVERWRITE');
 const save=(n,x)=>fs.writeFileSync(path.join(OUT,n),C.stringify(x));
 const scenarios=['strict','proxy'].flatMap(policy=>[{entryBlocks:4,exitBlocks:8},{entryBlocks:5,exitBlocks:10}].map(q=>({...q,policy,id:policy+'_'+q.entryBlocks+'_'+q.exitBlocks})));
 const plan={start,end,from:start-warmup,warmup,margin:100,leverage:500,initialVirtualCapital:2300,sourceProfile:profile,scenarios,assumptions:{visibilityMs:1000,cancelSuccessProbability:.13,entrySignedValidityMs:20000,price:'Own past HL BBO only; max1500ms/max6USD width; no summary-price fills',proxy:'External HL aggregate + own Binance best-level proxy; missing full L2 walls, rounded summary and inter-host timing remain unverified',unknown:'Freeze at first unavailable executable price; retain capital, no fake zero or later price'}};
 save('plan.json',plan);
 const replay=new Replay({start,end,split:start,warmupMs:warmup,profiles:scenarios.map(s=>({id:s.id,asset:'BTC',family:'y2',timing:'observe',label:profile.label+' / '+s.id,parameters:profile.parameters,execution:{preset:'live'}}))});replay.guards.clear();
 for(const m of replay.models){
  const s=scenarios.find(s=>s.id===m.id),e=m.engine;
  Object.assign(e.experiment,{margin:100,leverage:500,balance:'2300'});
  Object.assign(m.bridge.api.cfg,{margin:100,leverage:500,addMargin:0,maxPositions:1,splitParts:1,sizeToOi:false,live:true,enabled:true,useMainKey:false,tgEnabled:false,tgTrades:false,refEnabled:false,autoTune:false,autoProfile:false,schedEnabled:false,maxDayLossUsd:0});
  if(m.bridge.api.oi)m.bridge.api.oi.minNotional=Number(e.config.limits.minimumNotional);
  e.config.entryDeadline={signalTtlMs:profile.entrySignedValidityMs,safetySeconds:1};
  e.scenario={...e.scenario,congestion:0,volatility:0,volumeSlope:0,cancelMs:[200],cancelMinLeadMs:1500,cancelSuccessProbability:.13,fillFallbackMaxAgeMs:0,exitPriceRetryMs:0};
  e.delay=function(kind){if(kind==='cancel')return 200;return Math.max(1,Math.floor(this.now/1000)*1000+(kind==='entry'?s.entryBlocks:s.exitBlocks)*1000-this.now);};e.exitDelay=function(){return this.delay('exit');};
  m.owner.guard=()=>({status:'ALLOW_SHADOW',reason_codes:[]});
  m.entryGate=()=>e.accountingQuarantined?{allowed:false,reason:'ACCOUNTING_UNKNOWN'}:!e.inputHealthy||e.now<e.integrityWarmUntil?{allowed:false,reason:'WARMUP'}:hasRisk(e)?{allowed:false,reason:'ONE_SLOT'}:e.available()*95n<100n*10n**18n*100n?{allowed:false,reason:'MARGIN_RESERVE'}:{allowed:true};
  e.entryPolicyReason=()=>{const g=m.entryGate();return g.allowed?null:g.reason;};
  m.save=()=>{};m.censoredAt=null;
  m.note=function(row){row={...row,modelHash:m.hash};if(row.kind==='close_fill'&&m.censoredAt==null)replay.trades.get(m.id).push(row);
   replay.db.prepare('INSERT INTO sim_strategy_events(model_id,sequence,at_ms,kind,payload_json) VALUES(?,?,?,?,?)').run(m.id,row.sequence,row.time_ms,row.kind,C.stringify(row));
   if(['execution_price_unavailable','fill_price_unavailable','censored'].includes(row.kind)&&m.censoredAt==null){m.censoredAt=row.time_ms;m.unknownEvidence=row;m.frozen=true;e.accountingQuarantined=true;}
   return true;};
  installObservation(e,{entryObservationMs:[1000],exitObservationMs:[1000]});installMarkCache(e);
  const mark=e.mark.bind(e),fire=e.fire.bind(e),step=m.bridge.step;
  e.mark=t=>{if(m.frozen){e.now=t;m.bridge.setTime(t);return;}return mark(t);};
  e.fire=t=>{if(m.frozen)return;const result=fire(t);if(m.frozen&&m.unknownEvidence)require('./retain-unknown.cjs').retainUnknown(e,m.unknownEvidence);return result;};m.bridge.step=t=>{if(!m.frozen)return step(t);};
  m.hash=C.hash(C.stringify({plan,scenario:s,source:m.bridge.sourceManifest,adapterSha:crypto.createHash('sha256').update(fs.readFileSync(__filename)).digest('hex')}));
 }
 const {spawn}=require('node:child_process'),rl=require('node:readline'),child=spawn('python',['-u',path.join(__dirname,'hl-summary-export.py'),'--from-ms',String(start-warmup),'--to-ms',String(end),'--out',path.join(OUT,'input.json')],{stdio:['ignore','pipe','pipe']});
 let stderr='',n=0,last=null;child.stderr.on('data',x=>stderr=(stderr+String(x)).slice(-6000));const done=new Promise((resolve,reject)=>{child.on('error',reject);child.on('close',resolve);});
 try{
  for await(const line of rl.createInterface({input:child.stdout,crlfDelay:Infinity})){
   const e=JSON.parse(line);replay.advance(e.recv_ms);
   for(const m of replay.models)if(!m.frozen)m.engine.researchBooks?.observe(e);
   if(e.kind==='book_summary'){replay.events++;replay.last=e.recv_ms;replay.counts.summaries=(replay.counts.summaries??0)+1;}
   else replay.ingest(e);
   n++;last=e.recv_ms;
   if(n%200000===0)save('progress.json',{n,through:last,msk:new Date(last+10800000).toISOString(),models:replay.models.map(m=>({id:m.id,counts:m.engine.counts,net:Number(m.engine.realised)/1e18,censoredAt:m.censoredAt,checks:m.engine.lastInputChecks}))});
   if(n>=limit){child.kill();break;}
  }
  const code=await done;if(code!==0&&limit===Infinity)throw new Error('EXPORT_FAILED:'+stderr);
  const r=limit===Infinity?replay.finish():{status:'SMOKE_ONLY',results:replay.models.map(m=>replay.modelResult(m))};r.selected=null;r.plan=plan;r.executionScenarios=scenarios;r.limitsAreScenariosNotHardSessionStop=true;
  r.accountDetails=replay.models.map(m=>({id:m.id,censoredAt:m.censoredAt,unknownEvidence:m.unknownEvidence??null,frozen:m.frozen,errors:m.errors,lastError:m.lastError,counts:m.engine.counts,available:Number(m.engine.available())/1e18,reserved:Number(m.engine.reserved)/1e18,locked:Number(m.engine.locked)/1e18,realised:Number(m.engine.realised)/1e18,positions:[...m.engine.positions.values()],unresolved:[...m.engine.intents.values()].filter(r=>['queued','outcome_unknown'].includes(r.state)).map(r=>({id:r.id,state:r.state,kind:r.kind,executionAtMs:r.executionAtMs,priceMissing:r.executionPriceMissing})),tickStats:m.engine.tickStats,lastInputChecks:m.engine.lastInputChecks,summaryStats:m.engine.researchBooks?.stats??null}));
  save('result.json',r);replay.db.exec("VACUUM INTO '"+path.join(OUT,'ledger.sqlite').replaceAll("'","''")+"'");
  console.log(JSON.stringify({events:n,through:last,models:r.results.map(m=>({id:m.profile.id,metrics:m.all})),accounts:r.accountDetails.map(x=>({id:x.id,censoredAt:x.censoredAt,errors:x.errors}))}));
 }finally{child.kill();replay.close();}
}
main().catch(e=>{process.stderr.write(e.stack+'\n');process.exitCode=1;});
