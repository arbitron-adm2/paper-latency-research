'use strict';
const fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto');
const {ROOT}=require('./strategy.cjs');
function allowedRoots(){
  const roots=[path.join(ROOT,'reports')];
  // Isolated assignment exception, not a caller-selectable arbitrary filesystem root.
  const assignment=path.resolve(ROOT,'../assignment.json');
  if(path.basename(ROOT)==='work'&&fs.existsSync(assignment)){
    const a=JSON.parse(fs.readFileSync(assignment,'utf8'));
    if(a.workRoot===ROOT&&['shadow_acceptance','shadow_live_controls'].includes(a.role)&&a.outputRoot===path.resolve(ROOT,'../output'))roots.push(a.outputRoot);
  }
  return roots.filter(p=>fs.existsSync(p)).map(p=>fs.realpathSync(p));
}
function researchDirectory(dir){const r=fs.realpathSync(dir);if(!allowedRoots().some(root=>r===root||r.startsWith(root+path.sep)))throw new Error('Publication outside allowed research roots');return r;}
function createRunDirectory(requested){
  if(typeof requested!=='string')throw new Error('Explicit new --out required');
  const out=path.resolve(requested),parent=fs.realpathSync(path.dirname(out));
  if(!/^[a-zA-Z][a-zA-Z0-9._-]{0,95}$/.test(path.basename(out))||!allowedRoots().some(r=>parent===r||parent.startsWith(r+path.sep)))throw new Error('Output outside allowed research roots');
  fs.mkdirSync(out,{mode:0o700});return fs.realpathSync(out);
}
function safeChild(out,name){
  if(!/^[a-zA-Z][a-zA-Z0-9._-]{0,95}$/.test(name))throw new Error('Invalid research artifact name');
  const p=path.join(out,name);if(fs.existsSync(p)&&fs.lstatSync(p).isSymbolicLink())throw new Error('Symlink output denied');return p;
}
function writeNew(out,name,data){fs.writeFileSync(safeChild(out,name),typeof data==='string'?data:JSON.stringify(data,null,2)+'\n',{flag:'wx',mode:0o600});}
class TelemetryPublisher{
  constructor(out,name='telemetry.json'){
    if(name!=='telemetry.json')throw new Error('Telemetry filename is fixed inside the new research run');
    this.out=researchDirectory(out);this.target=safeChild(this.out,name);
  }
  publish(snapshot){
    safeChild(this.out,'telemetry.json');
    const temp=path.join(this.out,'.telemetry-'+crypto.randomBytes(8).toString('hex')+'.tmp');
    let fd;
    try{fd=fs.openSync(temp,'wx',0o600);fs.writeFileSync(fd,JSON.stringify(snapshot,null,2)+'\n');fs.fsyncSync(fd);fs.closeSync(fd);fd=null;fs.renameSync(temp,this.target);}
    finally{if(fd!==undefined&&fd!==null)fs.closeSync(fd);if(fs.existsSync(temp))fs.unlinkSync(temp);}
  }
}
class Audit{
  constructor(out,maxBytes=268435456){
    if(!Number.isSafeInteger(maxBytes)||maxBytes<1024||maxBytes>2147483648)throw new Error('Bounded audit size required');
    this.fd=fs.openSync(safeChild(researchDirectory(out),'audit.jsonl'),'wx',0o600);this.sequence=0;this.bytes=0;this.maxBytes=maxBytes;
  }
  append(kind,payload,atMs){
    const line=JSON.stringify({...payload,auditSequence:this.sequence,processedAtMs:atMs,auditType:kind,kind:payload.kind??kind,financialPerformance:kind==='simulated_execution'?'simulated':null})+'\n',n=Buffer.byteLength(line);
    if(this.bytes+n>this.maxBytes)throw new Error('AUDIT_SIZE_BOUND: stop, no silent dropping');
    fs.writeSync(this.fd,line);this.bytes+=n;this.sequence++;
  }
  close(){if(this.fd!==null){fs.fsyncSync(this.fd);fs.closeSync(this.fd);this.fd=null;}}
}
module.exports={allowedRoots,createRunDirectory,writeNew,TelemetryPublisher,Audit};
