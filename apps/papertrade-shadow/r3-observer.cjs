'use strict';
// Consume canonical R3 in this observer, with its transport and writer capabilities denied.
const fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const C=require('./state-codec.cjs');
const ROOT=path.resolve(__dirname,'../..');
const FILE=path.join(ROOT,'apps/paper-queue-latency/src/feed-guard.cjs');
const LATENCY=path.join(path.dirname(FILE),'latency.js');
function loadR3(){
  const deny=()=>{throw new Error('R3_OBSERVER_CAPABILITY_DENIED');},module={exports:{}};
  const read=p=>{if(![FILE,LATENCY].includes(path.resolve(p)))return deny();return fs.readFileSync(p);};
  const req=id=>{
    if(id==='node:http')return Object.freeze({request:deny});
    if(id==='node:crypto')return require('node:crypto');
    if(id==='node:path')return path;
    if(id==='node:fs')return Object.freeze({readFileSync:read,mkdirSync:deny,openSync:deny,writeFileSync:deny,renameSync:deny});
    if(id==='./latency.js')return require(LATENCY);
    return deny();
  };
  vm.runInNewContext(fs.readFileSync(FILE,'utf8'),{module,exports:module.exports,require:req,__filename:FILE,__dirname:path.dirname(FILE),Buffer,URL,
    setTimeout:deny,clearTimeout:deny,console:Object.freeze({log:()=>{},error:()=>{}})},{filename:FILE,timeout:2000,contextCodeGeneration:{strings:false,wasm:false}});
  return module.exports;
}
function captureGuard(g){const state={};for(const k of Object.keys(g))if(k!=='onPrediction'&&k!=='gate')state[k]=g[k];state.gate={};for(const k of Object.keys(g.gate))state.gate[k]=g.gate[k];return C.clone(state);}
function restoreGuard(g,state){const x=C.clone(state);const gate=x.gate;delete x.gate;Object.assign(g,x);Object.assign(g.gate,gate);}
module.exports={loadR3,captureGuard,restoreGuard,FILE,LATENCY};
