'use strict';
// A separate research copy of the reviewed bridge. Production bridge/files stay untouched.
const fs=require('node:fs'),path=require('node:path'),Module=require('node:module');
const ORIGINAL=require.resolve('../papertrade-shadow/strategy.cjs');
const SCANNER=path.join(path.dirname(ORIGINAL),'reviewed-baseline/apps/vchera2/scanner/btcy2-lag.js');
const INJECTION=`
// Research-only aggregate feature substitution; never writes feed prices or book levels.
const __rawResearchBookStat=bookStat;
bookStat=function(v){return __execution.researchBooks?.at(v,Date.now())||__rawResearchBookStat(v);};
const __rawResearchWallUpdate=SAFETY.walls.update.bind(SAFETY.walls);
SAFETY.walls.update=function(feeds,t=Date.now()){
 __rawResearchWallUpdate(feeds,t);
 for(const v of ['hl','bf','bs']){const s=__execution.researchBooks?.at(v,t);if(!s)continue;
  const ws=s.wall?[s.wall]:s.top?[{usd:s.top.bidUsd,side:'bid',dist:0},{usd:s.top.askUsd,side:'ask',dist:0}]:[];
  for(const w of ws){if(w.dist>this.settings.wallRangeUsd)continue;const against=w.side==='bid'?'S':'L';
   if(w.usd>=this.settings.wallAgainstUsd)this.until[against]=Math.max(this.until[against],t+this.settings.wallCooldownMs);
   if(w.usd>=this.settings.anyWallPauseUsd)this.until.any=Math.max(this.until.any,t+this.settings.anyWallPauseMs);
  }
 }
 return this.state(t);
};
`;
function createSummaryBridgeFactory(){
 const shim=Object.create(fs);shim.readFileSync=function(p,...args){const value=fs.readFileSync(p,...args);if(String(p)!==SCANNER)return value;return typeof value==='string'?value+INJECTION:Buffer.concat([value,Buffer.from(INJECTION)]);};
 let code=fs.readFileSync(ORIGINAL,'utf8');
 const old="const fullDepth=required.every(v=>f[v].bids.length&&f[v].asks.length&&t-f[v].bookT<=(v==='hl'?10000:3000));";
 if(code.split(old).length!==2)throw new Error('REVIEWED_BRIDGE_SHAPE_CHANGED');
 code=code.replace(old,"const actualFullDepth=required.every(v=>f[v].bids.length&&f[v].asks.length&&t-f[v].bookT<=(v==='hl'?10000:3000)); const fullDepth=required.every(v=>(f[v].bids.length&&f[v].asks.length&&t-f[v].bookT<=(v==='hl'?10000:3000))||!!engine.researchBooks?.at(v,t));");
 code=code.replace('engine.lastInputChecks={atMs:t,...flags,fullDepth,fresh};','engine.lastInputChecks={atMs:t,...flags,fullDepth,fresh,actualFullDepth,researchSummaryPolicy:engine.researchBooks?.policy??null};');
 const local=new Module(ORIGINAL,module),req=Module.createRequire(ORIGINAL);local.paths=Module._nodeModulePaths(path.dirname(ORIGINAL));local.require=id=>id==='node:fs'?shim:req(id);local._compile(code,ORIGINAL);
 return local.exports;
}
module.exports={createSummaryBridgeFactory,INJECTION};
