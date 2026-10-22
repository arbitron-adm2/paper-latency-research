'use strict';
// Reviewed Y2 rules only. HTTP, signatures and nonces remain in the one live owner.
const fs=require('node:fs'),crypto=require('node:crypto');
const {MainBtcRuntime}=require('./main-btc-runtime.cjs');
const {createBridge}=require('../papertrade-shadow/strategy.cjs');
const PROFILE=require('./y2-live-profile.json');
const PROFILE_HASH=crypto.createHash('sha256').update(fs.readFileSync(require.resolve('./y2-live-profile.json'))).digest('hex');
const ID=PROFILE.id;
function metadataDb(db){
 const sql=q=>String(q).replace(/\bsim_native_main_btc_live_/g,'sim_native_y2_live_').replace(/\bmain_btc_live_/g,'y2_live_');
 return{exec:q=>db.exec(sql(q)),prepare:q=>db.prepare(sql(q))};
}
function bridgeFactory(engine,db,start){
 const ns=q=>String(q).replace(/\bbtcy2_lag_/g,'sim_native_y2_live_');
 engine.config={persistentService:{enabled:true,acknowledgeSimulationOnly:true,cutoffPolicy:'continuous-observer-only-no-live'}};
 engine.persistentDb={exec:q=>db.exec(ns(q)),prepare:q=>db.prepare(ns(q))};
 engine.intents=new Map();engine.counts={localSignalExpired:0};engine.missingDepthTicks=0;engine.note=()=>{};
 const submit=engine.submit;
 engine.submit=q=>{const id=submit(q);if(id)engine.intents.set(id,{q});return id;};
 const bridge=createBridge(engine,require('../papertrade-shadow/baseline.json'),{margin:100,leverage:500,balance:'2300'},start);
 const market=bridge.market;
 // The in-process MarketData producer already validates quotes/depth. Its
 // events have no HTTP-envelope sequence, checksum or provenance fields.
 bridge.market=e=>market({kind:'market',market:e.symbol,venue:e.venue,recv_ms:e.recv_ms,event_ms:e.source_ms??e.recv_ms,
   source:'existing-public-market-data:'+e.venue,payload:e.payload,quality:{timestamp:e.source_ms==null?'receive-only':'source',depth:e.payload.frameKind==='book'?'observed':'missing'}});
 for(const name of ['terminal','fill']){const fn=bridge.api[name];bridge.api[name]=function(q,...args){const result=fn(q,...args);engine.intents.delete(q.intentId);return result;};}
 return bridge;
}
class Y2LiveRuntime extends MainBtcRuntime{
 constructor(options){super({...options,db:metadataDb(options.db),profile:PROFILE,profileHash:PROFILE_HASH,bridgeFactory});}
}
module.exports={Y2LiveRuntime,PROFILE,PROFILE_HASH,ID};
