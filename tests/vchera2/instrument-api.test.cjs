'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const {EventEmitter}=require('node:events');
const {createRequire}=require('node:module');
const filename=require.resolve('../../apps/vchera2/runner.cjs'),realRequire=createRequire(filename);
function harness(){
 const config=structuredClone(require('../../apps/vchera2/launch-config.json')),cfg={...config.strategy,maxFeedAgeMs:1500};
 const wallet={t:Date.now(),available:2300,balance:2300,connected:true,err:null,openPositions:[],operations:[]};const writes=[],settingsFiles=new Map();let handler,starts=0,stops=0;
 const bot={setConfig:patch=>Object.assign(cfg,patch),start(){},startTrading(){starts++;return{ok:true};},killSwitch:async()=>{stops++;return{ok:true};},getState:()=>({config:cfg,feeds:{hl:{ageMs:0},bf:{ageMs:0}},executionSync:{ready:true,t:Date.now()},open:[],events:[],guard:{tripped:false,dayPnl:-334.97}})};
 const provider={key:{active:true},wallet,entrySafety:{unknown:[]}};
 const liveModule=realRequire('./live-settings.cjs');
 class MemorySettings extends liveModule.LiveSettingsStore{constructor(options){super({...options,read:file=>{if(!settingsFiles.has(file)){const e=new Error();e.code='ENOENT';throw e;}return settingsFiles.get(file);},save:(file,value)=>settingsFiles.set(file,JSON.stringify(value))});}update(body,context,now){return super.update(JSON.parse(JSON.stringify(body)),context,now);}}
 const mocks={
  'node:fs':{openSync:()=>1,writeFileSync(){},closeSync(){},readFileSync:name=>{if(String(name).startsWith('/virtual')){const error=new Error();error.code='ENOENT';throw error;}return fs.readFileSync(name);}},
  'node:http':{createServer:fn=>{handler=fn;return{on(){},listen(p,h,fn){fn();},close(){}};}},
  './runtime.cjs':{privateRuntime:()=>'/virtual',atomicPrivateJSON:(file,value)=>writes.push({file,value})},
  './safety.cjs':{config,assertLaunchConfig(){},cutoffReached:()=>false,currentHighWater:()=>null,walls:{state:()=>({})}},
  './live-settings.cjs':{...liveModule,LiveSettingsStore:MemorySettings},
  './scanner/papertrade':{getAccount:()=>({status:()=>provider})},
  './scanner/funding-guard':{setCfg(){},getCfg:()=>({}),noEntry:()=>null,mustClose:()=>false},
  './scanner/btcy2-lag':bot,
  './db':{db:{prepare:sql=>({all:()=>[{position_id:'42',side:'S',entry:81541.5,exit:81569.5,closed_at:1,real_pnl:-432.66,reason:'late fill'}],get:()=>sql.includes('realisedUsd')?{closedTrades:13,pricedTrades:13,realisedUsd:-334.97}:{closedTrades:13,adjustedPnlUsd:-334.97,wins:4,losses:3}})}},
 };
 const sandbox={require:id=>Object.hasOwn(mocks,id)?mocks[id]:realRequire(id),__dirname:path.dirname(filename),process:{pid:12345,on(){},exit(){throw new Error('Unexpected exit');}},console:{log(){},error(){}},Date,Promise,Buffer,URL};
 vm.runInNewContext(fs.readFileSync(filename,'utf8'),sandbox,{filename});
 function begin(url,method='GET',headers={}){const req=new EventEmitter();Object.assign(req,{url,method,headers:{host:'127.0.0.1:18762',...headers}});const res={status:200,headers:{},setHeader(k,v){this.headers[k]=v;},writeHead(s){this.status=s;},end(body){this.body=body;this.done=true;}};handler(req,res);return{req,res};}
 function post(url,body,header=true){const request=begin(url,'POST',header?{'x-vchera2-control':'1'}:{});if(!request.res.done){request.req.emit('data',Buffer.from(typeof body==='string'?body:JSON.stringify(body)));request.req.emit('end');}return request.res;}
 return{begin,post,wallet,cfg,writes,settingsFiles,get starts(){return starts;},get stops(){return stops;}};
}
test('market API exposes both instrument contexts and separate canonical histories without trading',()=>{
 const h=harness();assert.equal(h.cfg.live,false);assert.equal(h.cfg.enabled,false);
 const res=h.begin('/api/markets').res;assert.equal(res.status,200);const s=JSON.parse(res.body);
 assert.deepEqual(Object.keys(s.markets),['BTC','ETH']);assert.equal(s.markets.BTC.history[0].symbol,'BTC');assert.equal(s.markets.ETH.history.length,0);
 assert.equal(s.markets.ETH.stats.adjustedPnlUsd,null);assert.equal(s.instrumentSelection.liveReady,false);assert.equal(h.starts,0);
});
test('only authorized instrument selection is persisted; protected start cannot run legacy engine',()=>{
 const h=harness();assert.equal(h.post('/api/instruments/selection',{mode:'ETH'},false).status,403);
 const selected=h.post('/api/instruments/selection',{mode:'BOTH'});assert.equal(selected.status,200);assert.equal(JSON.parse(selected.body).selection.mode,'BOTH');
 assert.equal(h.writes.filter(w=>w.file.endsWith('instrument-selection.json')).length,1);
 const start=h.post('/api/trading/start',{});assert.equal(start.status,409);assert.match(start.body,/deadline-guard/);assert.equal(h.starts,0);assert.equal(h.cfg.live,false);
});
test('selection rejects any exposure, extra control fields, invalid JSON and oversized body',()=>{
 const h=harness();h.wallet.openPositions=[{instrumentId:1}];assert.equal(h.post('/api/instruments/selection',{mode:'ETH'}).status,409);h.wallet.openPositions=[];
 assert.equal(h.post('/api/instruments/selection',{mode:'BTC',live:true}).status,400);
 assert.equal(h.post('/api/instruments/selection','{oops').status,400);assert.equal(h.post('/api/instruments/selection','x'.repeat(1025)).status,413);
 assert.equal(h.writes.filter(w=>w.file.endsWith('instrument-selection.json')).length,0);
});
test('unfinished selection body never holds the trading control lock',()=>{
 const h=harness();h.begin('/api/instruments/selection','POST',{'x-vchera2-control':'1'});
 const start=h.post('/api/trading/start',{});assert.equal(start.status,409);assert.match(start.body,/deadline-guard/);assert.doesNotMatch(start.body,/другая команда|Уже выполняется/i);
});
test('OFF runtime reports realised daily BTC PnL from the canonical journal',()=>{
 const h=harness(),s=JSON.parse(h.begin('/state').res.body);assert.equal(s.pnlDay.realisedUsd,-334.97);assert.equal(s.pnlDay.missingRealised,0);assert.match(s.pnlDay.basis,/Europe\/Moscow/);assert.equal(h.cfg.enabled,false);assert.equal(h.starts,0);
});
test('research endpoint is read-only, bounded and never routes to exchange commands',()=>{
 const h=harness(),response=h.begin('/api/research/events?after=0&limit=128').res;assert.equal(response.status,200);const body=JSON.parse(response.body);assert.equal(body.mode,'READ_ONLY_RESEARCH_NO_ORDERS');assert.equal(body.schemaVersion,1);assert.ok(Array.isArray(body.events));assert.equal(h.starts,0);
 assert.equal(h.post('/api/research/events',{}).status,405);
});
test('research endpoint rejects malformed/out-of-bounds cursors and unknown query fields',()=>{
 const h=harness();for(const url of ['/api/research/events?after=-1','/api/research/events?limit=999','/api/research/events?after=1.2','/api/research/events?wallet=private'])assert.equal(h.begin(url).res.status,400);
});
test('LIVE settings HTTP persists only a stopped draft, CAS version and no activation',()=>{
 const h=harness(),before=JSON.parse(h.begin('/api/live/settings').res.body);assert.equal(before.revision,0);assert.equal(before.settings.capital.marginUsdc,null);
 assert.equal(h.post('/api/live/settings',{expectedRevision:0,settings:before.settings},false).status,403);
 const draft=structuredClone(before.settings);draft.capital={budgetUsdc:'100',marginUsdc:'10',leverage:100};
 const response=h.post('/api/live/settings',{expectedRevision:0,settings:draft});assert.equal(response.status,200);const result=JSON.parse(response.body);assert.equal(result.ok,true);assert.equal(result.runtimeApplied,false);assert.equal(result.snapshot.activation.allowed,false);assert.equal(result.snapshot.wallet,undefined);
 assert.equal(result.snapshot.readiness.wallet.availableUsdc,'2300');assert.equal(h.cfg.margin,2100);assert.equal(h.starts,0);assert.equal(h.wallet.available,2300);
 assert.equal(h.post('/api/live/settings',{expectedRevision:0,settings:draft}).status,409);assert.equal(JSON.parse(h.begin('/api/live/settings').res.body).revision,1);
 const plan=JSON.parse(h.begin('/api/live/schedule-plan').res.body);assert.equal(plan.planOnly,true);assert.equal(plan.activationAllowed,false);
});
test('LIVE settings HTTP rejects unknown authority/safety flags, malformed/oversized requests and exposure',()=>{
 const h=harness(),draft=JSON.parse(h.begin('/api/live/settings').res.body).settings;
 assert.equal(h.post('/api/live/settings',{expectedRevision:0,settings:draft,authorized:true}).status,400);
 assert.equal(h.post('/api/live/settings','bad').status,400);assert.equal(h.post('/api/live/settings','x'.repeat(16385)).status,413);
 const unsafe=structuredClone(draft);unsafe.features.wallGuard=false;assert.equal(h.post('/api/live/settings',{expectedRevision:0,settings:unsafe}).status,400);
 h.wallet.openPositions=[{instrumentId:0}];assert.equal(h.post('/api/live/settings',{expectedRevision:0,settings:draft}).status,409);assert.equal(h.settingsFiles.size,0);
});
test('unfinished LIVE settings upload never blocks emergency trading control',()=>{
 const h=harness();h.begin('/api/live/settings','POST',{'x-vchera2-control':'1'});const start=h.post('/api/trading/start',{});assert.equal(start.status,409);assert.match(start.body,/deadline-guard/);assert.doesNotMatch(start.body,/CONTROL_BUSY/);
});
