'use strict';
// Single child executor owned by the existing dashboard. No supervisor, spawn,
// scheduler, external market collector, credentials, settings or order API.
process.env.TZ='Europe/Moscow';
const fs=require('node:fs'),path=require('node:path');
const {PersistentObserver}=require('./persistent-session.cjs');
const {LoopbackReader,ENDPOINT,validateSource}=require('./live-feed.cjs');
const {TelemetryPublisher}=require('./telemetry.cjs');
const {queryHistory,allowedDirectory,ownFile}=require('./history.cjs');
const {checkLockRecords}=require('./cli.cjs');
const {ROOT,sha}=require('./strategy.cjs');
const {hasRisk,censor}=require('./engine-state.cjs');
class PersistentService{
  constructor({stateDir,source=ENDPOINT,reader=null,clock=Date.now,historyFactory}={}){
    validateSource(source);this.clock=clock;this.source=source;
    this.observer=new PersistentObserver({stateDir,clock,comparisonEnabled:true,...(historyFactory?{historyFactory}: {})});
    this.reader=reader??new LoopbackReader({source,limit:256,timeoutMs:2000,maxBytes:2097152});
    this.publisher=new TelemetryPublisher(this.observer.dir);this.stopRequested=false;this.running=false;this.handlers=[];
    this.lastPublish=0;this.lastVerify=0;this.lastPollError=null;this.pendingFetch=null;this.pauseResolve=null;this.pauseTimer=null;
  }
  verify(){
    const p=this.observer.profile;checkLockRecords(p.locks);
    for(const r of p.implementation){const f=r.file.startsWith('apps/')?path.join(ROOT,r.file):path.join(__dirname,r.file);if(sha(fs.readFileSync(f))!==r.sha256)throw new Error('RUNNING_PROFILE_DRIFT:'+r.file);}
  }
  publish(){const value=this.status();this.publisher.publish(value);this.lastPublish=this.clock();return value;}
  status(){return this.finalStatus??this.observer.snapshot(this.clock());}
  async start(){
    if(this.running)throw new Error('SERVICE_ALREADY_STARTED');
    try{this.verify();}catch(e){await this.stop('STARTUP_PROFILE_REJECTED');throw e;}
    this.running=true;this.publisher.publish(this.status());
    this.heartbeatTimer=setInterval(()=>{
      if(this.stopRequested)return;
      try{
        const now=this.clock();this.observer.heartbeat(now);
        if(now-this.lastVerify>=5000){try{this.verify();}catch(e){this.quarantine(e.message,now);}this.lastVerify=now;}
        if(now-this.lastPublish>=1000)this.publish();
      }catch(e){this.quarantine('HEARTBEAT:'+e.message,this.clock());try{this.publish();}catch{}}
    },100);
    this.done=this.loop();return this;
  }
  quarantine(reason,now){
    const o=this.observer;o.failure=reason;o.recovery={...o.recovery,mode:'QUARANTINE_WARMUP',reason,requiresExplicitRecovery:true,censored:hasRisk(o.session.engine)};
    if(hasRisk(o.session.engine))censor(o.session.engine,reason);
    o.session.engine.entryBlockReasons=[...new Set([...(o.session.engine.entryBlockReasons??[]),reason])];
    o.session.invalidate(reason,Math.max(now,o.session.now));o.safeWrite(()=>o.history.quality(reason,{riskRetained:hasRisk(o.session.engine)},now));
  }
  async pause(ms){if(this.stopRequested)return;await new Promise(resolve=>{this.pauseResolve=()=>{clearTimeout(this.pauseTimer);this.pauseResolve=null;resolve();};this.pauseTimer=setTimeout(this.pauseResolve,ms);});}
  fetchOne(){return new Promise((resolve,reject)=>{
    let finished=false;const done=(error,value)=>{if(finished)return;finished=true;this.pendingFetch=null;error?reject(error):resolve(value);};
    this.pendingFetch=()=>done(null,null);
    Promise.resolve().then(()=>this.stopRequested?null:this.reader.get(this.observer.continuity.cursor)).then(x=>done(null,x),e=>done(e));
  });}
  async loop(){
    try{
      while(!this.stopRequested){
        let response;
        try{this.observer.stats.requests++;response=await this.fetchOne();}
        catch(e){
          if(this.stopRequested)break;
          this.lastPollError=e.message;this.observer.guard.fetchError('LOOPBACK_GET_FAILED',this.clock());this.quarantine('FEED_UNAVAILABLE',this.clock());
          this.observer.status='FEED_UNAVAILABLE';try{this.publish();}catch{}
          await this.pause(1000);continue;
        }
        if(this.stopRequested||response===null)break;
        try{this.observer.batch(response,this.clock());this.lastPollError=null;}
        catch(e){this.quarantine('FEED_CONTRACT:'+e.message,this.clock());this.observer.status='FEED_QUARANTINED';try{this.publish();}catch{}await this.pause(1000);}
        await this.pause(response.events.length>=256&&this.observer.continuity.cursor<response.lastSequence?0:50);
      }
    }finally{
      clearInterval(this.heartbeatTimer);this.running=false;
      try{this.finalStatus=this.observer.stop(this.stopReason??'PARENT_STOP',this.clock());}
      catch(e){this.observer.failure='STOP_STORAGE_ERROR:'+e.message;this.observer.status='ERROR';try{this.finalStatus=this.observer.snapshot(this.clock());}catch{this.finalStatus={schemaVersion:1,mode:'READ_ONLY_RESEARCH_NO_ORDERS',financialMutations:0,realSignatures:0,generatedAtMs:this.clock(),runId:this.observer.runId,status:'ERROR',input:{},assets:{BTC:{accounting:'UNKNOWN_PRESERVED_IN_DB'}},acceptance:{performanceVerdict:'INCONCLUSIVE'}};}try{this.observer.session.close();}catch{}try{this.observer.history.close();}catch{}}
      try{this.publisher.publish(this.finalStatus);}catch(e){this.lastPollError='STATUS_PUBLISH_FAILED:'+e.message;}finally{this.detachLifecycle();}
    }
    return this.finalStatus;
  }
  async stop(reason='PARENT_STOP'){
    if(this.stopRequested)return this.done??this.finalStatus;
    this.stopRequested=true;this.stopReason=reason;clearInterval(this.heartbeatTimer);this.pendingFetch?.();this.pauseResolve?.();this.reader.close?.();
    if(this.done)return this.done;
    this.finalStatus=this.observer.stop(reason,this.clock());this.publisher.publish(this.finalStatus);this.detachLifecycle();return this.finalStatus;
  }
  checkpoint(){return this.observer.saveCheckpoint(false,this.clock());}
  exportHistory({limit=20}={}){return queryHistory(this.observer.dir,{limit});}
  attachLifecycle(target=process){
    if(this.handlers.length)throw new Error('LIFECYCLE_ALREADY_ATTACHED');
    const reply=(requestId,type,value)=>{if(target.connected&&typeof target.send==='function')try{target.send({type,requestId,...value});}catch{}};
    const stop=()=>{this.stop('PARENT_SIGNAL').then(()=>{if(target.connected)target.disconnect?.();}).catch(e=>{reply(null,'shadow_error',{error:e.message});if(target.connected)target.disconnect?.();});};
    const message=m=>{
      if(!m||typeof m!=='object'||typeof m.type!=='string')return;
      const id=typeof m.requestId==='string'&&m.requestId.length<=128?m.requestId:null;
      try{
        if(m.type==='status')reply(id,'shadow_status',{status:this.status()});
        else if(m.type==='stop'){this.stop('PARENT_IPC').then(x=>{reply(id,'shadow_stopped',{status:x});if(target.connected)target.disconnect?.();});}
        else if(m.type==='history')reply(id,'shadow_history',{history:this.exportHistory({limit:m.limit??20})});
        else if(m.type==='checkpoint')reply(id,'shadow_checkpoint',{saved:this.checkpoint()});
        else if(m.type==='research-recovery')reply(id,'shadow_recovery',{recovery:this.observer.explicitRecovery(m.ack,this.clock())});
        else if(m.type==='add-sim-strategy'){const result=this.observer.suite.addStrategy(m.definition,this.clock());this.publish();reply(id,'shadow_strategy_added',{result});}
        else reply(id,'shadow_error',{error:'UNSUPPORTED_CONTROL_NO_SETTINGS_OR_LIVE_ACTION'});
      }catch(e){reply(id,'shadow_error',{error:e.message});}
    };
    for(const [event,fn] of [['SIGTERM',stop],['SIGINT',stop],['message',message]]){target.on(event,fn);this.handlers.push({target,event,fn});}
    return this;
  }
  detachLifecycle(){for(const {target,event,fn} of this.handlers)target.removeListener(event,fn);this.handlers=[];}
}
function parseOptions(argv){const out={};for(let i=0;i<argv.length;i+=2){if(!argv[i]?.startsWith('--')||!argv[i+1]||argv[i+1].startsWith('--'))throw new Error('Expected --state-dir/--source/--limit');const k=argv[i].slice(2);if(!['state-dir','source','limit'].includes(k)||k in out)throw new Error('FIXED_PRESET_NO_USER_KNOBS');out[k]=argv[i+1];}if(!out['state-dir'])throw new Error('--state-dir required');return out;}
async function startService(options){const service=new PersistentService(options);await service.start();return service;}
async function main(argv=process.argv.slice(2)){
  const [command,...rest]=argv,o=parseOptions(rest);
  if(command==='upgrade-flat-profile'){if(o.source||o.limit)throw new Error('UPGRADE_HAS_NO_NETWORK');const observer=new PersistentObserver({stateDir:o['state-dir'],flatUpgrade:true,comparisonEnabled:true});const status=observer.stop('EXPLICIT_RESEARCH_PROFILE_UPGRADE');new TelemetryPublisher(observer.dir).publish(status);console.log(JSON.stringify({upgraded:true,runId:status.runId,cashPreserved:true,modelHash:status.runtime.modelHash}));return;}
  if(command==='history'||command==='export-history'){
    if(o.source)throw new Error('HISTORY_HAS_NO_NETWORK');console.log(JSON.stringify(queryHistory(o['state-dir'],{limit:o.limit==null?20:Number(o.limit)}),null,2));return;
  }
  if(command==='status'){
    if(o.source||o.limit)throw new Error('STATUS_OPTIONS');const dir=allowedDirectory(o['state-dir']),f=ownFile(dir,'telemetry.json');if(fs.statSync(f).size>2097152)throw new Error('STATUS_SIZE_BOUND');console.log(fs.readFileSync(f,'utf8'));return;
  }
  if(command!=='start'||o.limit)throw new Error('Commands: start | status | history | export-history');
  const service=new PersistentService({stateDir:o['state-dir'],source:o.source??ENDPOINT});service.attachLifecycle(process);await service.start();
  console.log(JSON.stringify({mode:'READ_ONLY_RESEARCH_NO_ORDERS',service:'persistent-shadow',pid:process.pid,runId:service.observer.runId,notionalUsd:1260000,telemetry:path.join(service.observer.dir,'telemetry.json'),financialMutations:0,realSignatures:0}));
  return service;
}
if(require.main===module)main().catch(e=>{console.error('PERSISTENT_SHADOW_BLOCKED:',e.message);process.exitCode=1;});
module.exports={PersistentService,startService,main,parseOptions,queryHistory};
