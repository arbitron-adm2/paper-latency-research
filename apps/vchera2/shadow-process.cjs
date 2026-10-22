'use strict';
// Owns only a read-only research child. No timers/schedules or trading transport.
const fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto');
const {spawn}=require('node:child_process');
class ShadowProcess{
 constructor({root,script,args=[],sourceHash,spawnImpl=spawn,now=Date.now}){
  this.root=path.resolve(root);this.script=path.resolve(script);this.args=[...args];this.sourceHash=sourceHash;this.spawnImpl=spawnImpl;this.now=now;
  this.child=null;this.startedAtMs=null;this.endedAtMs=null;this.lastError=null;this.lastOutput='';this.stopping=false;this.exits=0;this.pending=new Map();
 }
 start(){
  if(this.child)return this.status();
  const real=fs.realpathSync(this.script),allowed=path.join(this.root,'apps/papertrade-shadow')+path.sep;
  if(!real.startsWith(allowed)||!real.endsWith('.cjs')||fs.lstatSync(this.script).isSymbolicLink())throw new Error('Unreviewed shadow executable path');
  if(this.sourceHash&&crypto.createHash('sha256').update(fs.readFileSync(real)).digest('hex')!==this.sourceHash)throw new Error('Reviewed shadow worker changed; refusing spawn');
  const env={};for(const key of['PATH','HOME','TZ','LANG','LC_ALL','NODE_ENV'])if(process.env[key])env[key]=process.env[key];
  env.PAPER_SHADOW_READ_ONLY='1';
  const child=this.spawnImpl(process.execPath,[real,...this.args],{cwd:this.root,env,stdio:['ignore','pipe','pipe','ipc']});
  this.child=child;this.startedAtMs=this.now();this.endedAtMs=null;this.lastError=null;this.stopping=false;
  const output=chunk=>{this.lastOutput=(this.lastOutput+String(chunk)).slice(-8192);};
  child.stdout?.on('data',output);child.stderr?.on('data',output);
  child.on('message',m=>{const p=this.pending.get(m?.requestId);if(!p)return;clearTimeout(p.timer);this.pending.delete(m.requestId);m.type==='shadow_error'?p.reject(new Error(m.error)):p.resolve(m.result);});
  child.once('error',error=>{this.lastError=String(error.message).slice(0,500);if(!child.pid&&this.child===child){this.child=null;this.endedAtMs=this.now();}});
  child.once('exit',(code,signal)=>{if(this.child===child)this.child=null;for(const p of this.pending.values()){clearTimeout(p.timer);p.reject(new Error('SHADOW_STOPPED_RECONCILE_HISTORY'));}this.pending.clear();this.endedAtMs=this.now();this.exits++;if(!this.stopping&&code!==0)this.lastError=`Shadow exited (${code??signal}). Needs explicit restart, history not cleared.`;});
  return this.status();
 }
 addStrategy(definition){
  const child=this.child;if(!child?.connected||this.stopping)return Promise.reject(new Error('SHADOW_NOT_CONNECTED'));
  const requestId=crypto.randomUUID();
  return new Promise((resolve,reject)=>{const timer=setTimeout(()=>{this.pending.delete(requestId);reject(new Error('STRATEGY_RESULT_UNKNOWN_CHECK_LIST'));},8000);this.pending.set(requestId,{resolve,reject,timer});child.send({type:'add-sim-strategy',requestId,definition},error=>{if(error){clearTimeout(timer);this.pending.delete(requestId);reject(new Error('STRATEGY_DELIVERY_UNKNOWN_CHECK_LIST'));}});});
 }
 stop(){
  const child=this.child;if(!child)return Promise.resolve(this.status());
  this.stopping=true;
  return new Promise(resolve=>{
   const done=()=>resolve(this.status());child.once('exit',done);
   if(child.exitCode!==null&&child.exitCode!==undefined)return done();
   // Only this exact owned child; never infer or kill a PID from a stale file.
   child.kill('SIGTERM');
  });
 }
 status(){return{kind:'PERSISTENT_SHADOW_CHILD',mode:'READ_ONLY_RESEARCH_NO_ORDERS',running:!!this.child,pid:this.child?.pid||null,startedAtMs:this.startedAtMs,endedAtMs:this.endedAtMs,stopping:this.stopping,exits:this.exits,lastError:this.lastError,lastOutput:this.lastOutput,actualTradingPermission:false,automaticRetry:false};}
}
module.exports={ShadowProcess};
