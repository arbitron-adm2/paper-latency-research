'use strict';
// Pinned main-BTC rules, fed by the existing public stream. The VM cannot send,
// sign, fetch or start timers; all execution stays in the existing live owner.
const fs=require('node:fs'),crypto=require('node:crypto');
const {createNativeBridge}=require('../papertrade-shadow/native-bridge.cjs');
const C=require('../papertrade-shadow/state-codec.cjs');
const PROFILE=require('./main-btc-profile.json');
const PROFILE_HASH=crypto.createHash('sha256').update(fs.readFileSync(require.resolve('./main-btc-profile.json'))).digest('hex');
const ID=PROFILE.id;
class MainBtcRuntime {
 constructor({db,marketData,margin=100,leverage=500,now=Date.now,profile=PROFILE,profileHash=PROFILE_HASH,bridgeFactory=null}){
  this.db=db;this.now=now;this.startedAt=now();this.margin=margin;this.leverage=leverage;
  this.profile=profile;this.profileHash=profileHash;this.id=profile.id;
  this.requests=[];this.cancels=[];this.closes=[];this.error=null;this.acceptedEvents=0;this.rejectedEvents=0;this.lastDecision=null;
  this.entryEnabled=false;this.lastSave=0;this.lastStep=0;
  db.exec(`CREATE TABLE IF NOT EXISTS main_btc_live_intents(intent_id TEXT PRIMARY KEY,signal_ms INTEGER NOT NULL,valid_until_ms INTEGER NOT NULL,ctx_json TEXT NOT NULL,margin REAL NOT NULL,leverage INTEGER NOT NULL,side TEXT NOT NULL,state TEXT NOT NULL,position_id TEXT);
    CREATE TABLE IF NOT EXISTS main_btc_live_checkpoint(id INTEGER PRIMARY KEY CHECK(id=1),at_ms INTEGER NOT NULL,payload_json TEXT NOT NULL)`);
  this.engine={balance:2300n*10n**18n,inputHealthy:true,integrityWarmUntil:this.startedAt+this.profile.warmupMs,
   available:()=>this.engine.balance,
   recordDecision:(strategy,side,sig,ok,decision)=>{this.lastDecision={...decision,strategy,side,sig,ok};},
   submit:q=>{if(!this.entryEnabled)return null;const id=this.id+'-'+crypto.randomUUID();this.requests.push({...q,requestId:id});return id;},
   cancel:(id,reason)=>{this.cancels.push({requestId:id,reason});},
   close:p=>{this.closes.push({positionId:String(p.positionId),why:p.closing?.why});return 'existing-live-owner:'+p.positionId;}};
  this.bridge=bridgeFactory?bridgeFactory(this.engine,db,this.startedAt):createNativeBridge(this.engine,{asset:'BTC',namespace:'sim_native_main_btc_live'},db,this.startedAt);
  if(C.stringify(this.bridge.sourceManifest)!==C.stringify(this.profile.sourceManifest))throw new Error('LIVE_STRATEGY_SOURCE_PIN_CHANGED');
  Object.assign(this.bridge.api.cfg,this.profile.parameters,{margin,leverage,addMargin:0,maxPositions:1,splitParts:1,sizeToOi:false,
   live:true,enabled:true,useMainKey:false,tgEnabled:false,tgTrades:false,refEnabled:false,autoTune:false,autoProfile:false,schedEnabled:false,maxDayLossUsd:0});
  this.savedPositions=new Map();
  const saved=db.prepare('SELECT payload_json FROM main_btc_live_checkpoint WHERE id=1').get();
  if(saved){const s=C.parse(saved.payload_json);if(s.profileHash!==this.profileHash)throw new Error('LIVE_CHECKPOINT_PROFILE_CHANGED');this.savedPositions=s.positions??new Map();}
  this.detach=marketData.onEvent(e=>{try{this.ingest(e);}catch(e){this.error=e.message;}});
 }
 ingest(e){
  if(e.kind!=='market'||e.symbol!=='BTC')return;
  const now=this.now();
  if(!['hl','bf','bs'].includes(e.venue)||!['quote','book'].includes(e.payload?.frameKind)||now-e.recv_ms>2500||now-e.recv_ms< -1000||e.source_ms!=null&&e.source_ms>e.recv_ms+1000){this.rejectedEvents++;return;}
  this.bridge.market(e);this.acceptedEvents++;
 }
 blockers(){
  const now=this.now(),out=[];
  if(this.error)out.push('MAIN_BTC_RUNTIME:'+this.error);
  if(now<this.startedAt+this.profile.warmupMs)out.push('LIVE_STRATEGY_WARMUP_'+Math.ceil((this.startedAt+this.profile.warmupMs-now)/1000)+'S');
  for(const v of ['hl','bf']){const f=this.bridge.api.feed[v];if(!(f.t>0)||now-f.t>1500)out.push('MAIN_BTC_QUOTE_STALE:'+v);if(!f.bids.length||!f.asks.length||now-f.bookT>(v==='hl'?10000:3000))out.push('MAIN_BTC_BOOK_STALE:'+v);}
  const missing=this.db.prepare("SELECT COUNT(*) n FROM btcy2_lag_trades WHERE strategy=? AND live=1 AND status='closed' AND qty>0 AND real_pnl IS NULL").get(this.id).n;
  if(missing)out.push('MAIN_BTC_CLOSED_PNL_NOT_RECONCILED');
  return out;
 }
 remember(q){
  this.db.prepare('INSERT INTO main_btc_live_intents VALUES(?,?,?,?,?,?,?,?,NULL)').run(q.intentId.toLowerCase(),q.createdAt,q.validUntilMs,JSON.stringify(q.ctx),q.margin,q.leverage,q.side,'prepared');
 }
 mark(intent,state,position=null){if(intent)this.db.prepare('UPDATE main_btc_live_intents SET state=?,position_id=COALESCE(?,position_id) WHERE intent_id=?').run(state,position,String(intent).toLowerCase());}
 recover({pending,positions,cancelledLive,provider}){
  const ids=new Set([...(provider.entrySafety?.unknown??[]).map(r=>String(r.intentId).toLowerCase()),...(provider.wallet.operations??[]).map(o=>String(o[0]).toLowerCase()),...(provider.wallet.openPositions??[]).map(p=>String(p.intentId).toLowerCase())]);
  for(const r of this.db.prepare("SELECT * FROM main_btc_live_intents WHERE state='prepared'").all()){
   if(!ids.has(r.intent_id)||pending.some(q=>q.intentId?.toLowerCase()===r.intent_id)||cancelledLive.has(r.intent_id)||[...positions.values()].some(p=>p.intentId?.toLowerCase()===r.intent_id))continue;
   const ctx=JSON.parse(r.ctx_json);pending.push({strategy:this.id,side:r.side,ctx,sig:ctx.sig,margin:r.margin,leverage:r.leverage,createdAt:r.signal_ms,validUntilMs:r.valid_until_ms,entryMaxWaitMs:this.profile.entryMaxWaitMs,fillAt:Infinity,px:ctx.hl,live:true,intentId:r.intent_id,rank:null,recovered:true});
  }
 }
 sync({positions,pending,provider}){
  const api=this.bridge.api,now=this.now();this.engine.balance=BigInt(Math.floor(Math.max(0,Number(provider.wallet.available)||0)*1e6))*10n**12n;
  for(const p of positions.values()){
   if(!p.live||p.strategy!==this.id||[...api.positions.values()].some(n=>String(n.positionId)===String(p.positionId)))continue;
   const row=this.db.prepare('SELECT * FROM main_btc_live_intents WHERE intent_id=?').get(String(p.intentId).toLowerCase());
   if(!row){this.error='POSITION_WITHOUT_EXACT_INTENT_METADATA';continue;}
   const ctx=JSON.parse(row.ctx_json),saved=this.savedPositions.get(String(p.positionId));
   if(saved){api.positions.set(saved.id,saved);this.savedPositions.delete(String(p.positionId));}
   else {
    const existing=this.db.prepare('SELECT * FROM sim_native_main_btc_live_trades WHERE position_id=? AND status=\'open\' ORDER BY id DESC LIMIT 1').get(String(p.positionId));
    if(existing){api.positions.set(existing.id,{...p,id:existing.id,strategy:'spread',bBest:(p.side==='L'?1:-1)*(api.feed.bf.bid+api.feed.bf.ask)/2});}
    else {const q=api.pending.find(q=>q.intentId===ctx.nativeRequestId)??{strategy:'spread',side:p.side,ctx,createdAt:row.signal_ms,px:ctx.hl};api.fill(q,{positionId:String(p.positionId),intentId:String(p.intentId),entry:p.entry,margin:p.margin,leverage:p.leverage,openedAt:p.opened_at});}
   }
   this.mark(p.intentId,'filled',String(p.positionId));
  }
  for(const q of [...api.pending]){
   const active=pending.find(p=>p.strategy===this.id&&p.ctx?.nativeRequestId===q.intentId);
   if(active){q.signalGone=!!active.signalGone;q.missed=!!active.missed;continue;}
   const row=this.db.prepare("SELECT * FROM main_btc_live_intents WHERE json_extract(ctx_json,'$.nativeRequestId')=? ORDER BY signal_ms DESC LIMIT 1").get(q.intentId);
   if(!row||['not_sent','closed','rejected'].includes(row.state)||now>row.valid_until_ms+30000&&!provider.entrySafety?.unknown?.length&&!provider.wallet.operations.length&&!provider.wallet.openPositions.length&&provider.wallet.t>row.valid_until_ms+30000)api.terminal(q);
  }
  if(now-this.lastSave>=1000&&api.positions.size)this.save();
 }
 save(){const positions=new Map([...this.bridge.api.positions.values()].map(p=>[String(p.positionId),p]));this.db.prepare('INSERT INTO main_btc_live_checkpoint VALUES(1,?,?) ON CONFLICT(id) DO UPDATE SET at_ms=excluded.at_ms,payload_json=excluded.payload_json').run(this.now(),C.stringify({profileHash:this.profileHash,positions}));this.lastSave=this.now();}
 closed(p,evidence){
  for(const n of [...this.bridge.api.positions.values()])if(String(n.positionId)===String(p.positionId)){
   this.bridge.api.closePx.set(n.positionId,{px:evidence.px,ts:evidence.ts,realPnl:evidence.realPnl});
   this.bridge.api.closePosition(n,p.closing?.why??'confirmed close',evidence.px,evidence.ts);
   if(Number.isFinite(evidence.realPnl))this.db.prepare('UPDATE sim_native_main_btc_live_trades SET real_pnl=?,est_pnl=? WHERE id=?').run(evidence.realPnl,evidence.realPnl,n.id);
  }
  this.mark(p.intentId,'closed',String(p.positionId));this.save();
 }
 advance(now,entryEnabled){
  this.entryEnabled=!!entryEnabled&&this.blockers().length===0;
  try{this.bridge.step(now);}catch(e){this.error=e.message;}
  this.entryEnabled=false;
 }
 snapshot(){const reasons=this.blockers();return{id:this.id,label:this.profile.label,profileHash:this.profileHash,shadowModelId:this.profile.shadowModelId,ready:reasons.length===0,warmup:this.now()<this.startedAt+this.profile.warmupMs,liveInputsReady:!reasons.some(r=>r.includes('STALE')),error:this.error,reason:reasons.join('; ')||this.lastDecision?.why||'Наблюдает',lastDecision:this.lastDecision,acceptedEvents:this.acceptedEvents,rejectedEvents:this.rejectedEvents,source:this.profile.sourceManifest,parameters:this.profile.parameters,realOrdersVia:'existing btcy2 live owner',fillPriceFallback:false};}
}
module.exports={MainBtcRuntime,PROFILE,PROFILE_HASH,ID};
