'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),os=require('node:os'),path=require('node:path'),crypto=require('node:crypto');
const {EventEmitter}=require('node:events');
const {ShadowProcess}=require('../../apps/vchera2/shadow-process.cjs');
function setup(){const root=fs.mkdtempSync(path.join(os.tmpdir(),'shadow-owner-'));const dir=path.join(root,'apps/papertrade-shadow');fs.mkdirSync(dir,{recursive:true});const script=path.join(dir,'service.cjs');fs.writeFileSync(script,'// reviewed fake worker\n');const calls=[];let child;
 const spawnImpl=(...args)=>{calls.push(args);child=new EventEmitter();child.pid=77777;child.exitCode=null;child.stdout=new EventEmitter();child.stderr=new EventEmitter();child.kills=[];child.kill=signal=>{child.kills.push(signal);return true;};return child;};
 const controller=new ShadowProcess({root,script,args:['run','--readonly'],sourceHash:crypto.createHash('sha256').update(fs.readFileSync(script)).digest('hex'),spawnImpl});return{root,script,calls,controller,child:()=>child,cleanup:()=>fs.rmSync(root,{recursive:true,force:true})};}
test('controller starts only one owned read-only child with secret-free allowlisted environment',()=>{
 const h=setup();try{const a=h.controller.start();h.controller.start();assert.equal(h.calls.length,1);assert.equal(a.pid,77777);assert.equal(a.actualTradingPermission,false);
 const opts=h.calls[0][2];assert.equal(opts.cwd,h.root);assert.equal(opts.env.PAPER_SHADOW_READ_ONLY,'1');assert.ok(Object.keys(opts.env).every(k=>['PATH','HOME','TZ','LANG','LC_ALL','NODE_ENV','PAPER_SHADOW_READ_ONLY'].includes(k)));assert.equal(opts.stdio[0],'ignore');}finally{h.cleanup();}
});
test('stop signals only the exact child and waits for graceful exit; no automatic retry',async()=>{
 const h=setup();try{h.controller.start();const child=h.child(),stopping=h.controller.stop();assert.deepEqual(child.kills,['SIGTERM']);child.exitCode=0;child.emit('exit',0,null);const done=await stopping;assert.equal(done.running,false);assert.equal(h.calls.length,1);assert.equal(done.exits,1);}finally{h.cleanup();}
});
test('bad code hash and symlink cannot start an unreviewed worker',()=>{
 const h=setup();try{fs.appendFileSync(h.script,'// changed');assert.throws(()=>h.controller.start(),/changed/);assert.equal(h.calls.length,0);
 const link=path.join(path.dirname(h.script),'link.cjs');fs.symlinkSync(h.script,link);const c=new ShadowProcess({root:h.root,script:link,spawnImpl:()=>{throw new Error('must not spawn');}});assert.throws(()=>c.start(),/Unreviewed/);}finally{h.cleanup();}
});
test('failed process exits remain visible, output bounded, no crash/restart watchdog created',()=>{
 const h=setup();try{h.controller.start();const child=h.child();child.stderr.emit('data','x'.repeat(20000));child.emit('exit',1,null);const s=h.controller.status();assert.equal(s.running,false);assert.equal(s.lastOutput.length,8192);assert.match(s.lastError,/Needs explicit restart/);assert.equal(s.automaticRetry,false);assert.equal(h.calls.length,1);}finally{h.cleanup();}
});
test('hot strategy IPC correlates acknowledgement from the existing owned child',async()=>{const h=setup();try{h.controller.start();const child=h.child();child.connected=true;child.send=(message,cb)=>{assert.equal(message.type,'add-sim-strategy');assert.equal(message.definition.template,'eth-native');cb?.(null);queueMicrotask(()=>child.emit('message',{requestId:message.requestId,type:'shadow_strategy_added',result:{ok:true,model:{id:'custom_test'}}}));};const r=await h.controller.addStrategy({template:'eth-native'});assert.equal(r.ok,true);assert.equal(h.calls.length,1);assert.equal(h.controller.pending.size,0);}finally{h.cleanup();}});
