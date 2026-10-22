'use strict';
// A separate scenario for one-price CSV snapshots. Never represents a BBO.
const fs=require('node:fs'),path=require('node:path'),Module=require('node:module');
function createSummaryExecutionClass(){
 const file=require.resolve('../papertrade-shadow/engine.cjs');let code=fs.readFileSync(file,'utf8');
 for(const name of ['q','priceEvidence']){
  const before='('+name+'.bid+'+name+'.ask)/2';
  if(code.split(before).length!==2)throw new Error('REVIEWED_EXECUTION_SHAPE_CHANGED:'+name);
  code=code.replace(before,'('+name+'.priceKind===\'external-summary-mid\'?'+name+'.mid:('+name+'.bid+'+name+'.ask)/2)');
 }
 const local=new Module(file,module);local.paths=Module._nodeModulePaths(path.dirname(file));local.require=Module.createRequire(file);local._compile(code,file);return local.exports.Execution;
}
function installSummaryExecutionPrice(engine,books,{maxAgeMs=1500}={}){
 const original=engine.executionQuote.bind(engine);engine.config.executionPricePolicy={...engine.config.executionPricePolicy,id:'own-bbo-or-explicit-external-summary-mid-v1'};
 engine.summaryPriceCounts={ownBbo:0,externalMid:0,unavailable:0};
 engine.executionQuote=function(at){
  const q=original(at);if(q){this.summaryPriceCounts.ownBbo++;return q;}
  const s=books.summary;
  if(!s||s.at>at||at-s.at>maxAgeMs||!(s.mid>0)){this.summaryPriceCounts.unavailable++;return null;}
  this.summaryPriceCounts.externalMid++;
  return{priceKind:'external-summary-mid',mid:s.mid,snapshotMs:s.at,ageMs:at-s.at,executionAtMs:at,sourceRecordId:s.sourceRecordId,sourceId:s.sourceId,
    policy:'own-bbo-or-explicit-external-summary-mid-v1',priceModel:'External client HL mid snapshot; no BBO spread or independent source age; conditional fill, not Paper evidence'};
 };
}
module.exports={createSummaryExecutionClass,installSummaryExecutionPrice};
