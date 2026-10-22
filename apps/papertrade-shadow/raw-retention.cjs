'use strict';
const fs=require('node:fs'),path=require('node:path'),zlib=require('node:zlib'),crypto=require('node:crypto');
const C=require('./state-codec.cjs');
class RawRetention{
  constructor(history,policy,{onWarning=()=>{},onRecovered=()=>{}}={}){
    this.history=history;this.policy=policy;this.onWarning=onWarning;this.onRecovered=onRecovered;this.dir=path.join(history.dir,'raw');
    if(!fs.existsSync(this.dir))fs.mkdirSync(this.dir,{mode:0o700});
    if(fs.lstatSync(this.dir).isSymbolicLink())throw new Error('RAW_DIRECTORY_SYMLINK');
    this.buffer=[];this.bufferBytes=0;this.chunk=[];this.chunkBytes=0;this.sampleSeconds=new Map();this.lastTriggers=new Map();this.contextUntil=0;this.contextKeys=[];this.lastGlobalContext=0;
    this.stats={seen:0,written:0,sampled:0,contextEvents:0,plannedOmitted:0,oversizeOmitted:0,storageLost:0,contextsSuppressed:0,segments:0,orphanFiles:0};this.disabled=false;this.warning=null;
    const known=new Set(history.db.prepare('SELECT file FROM raw_segments WHERE deleted_ms IS NULL').all().map(x=>path.basename(x.file)));
    for(const name of fs.readdirSync(this.dir))if(!known.has(name)){this.stats.orphanFiles++;this.warn('RAW_ORPHAN_REQUIRES_REVIEW');}
  }
  warn(code){this.warning=code;this.onWarning(code);}
  observe(event,now,boot){
    this.stats.seen++;
    const row=JSON.stringify({bootId:boot,processedAtMs:now,event,hashKind:'CANONICAL_LOCAL_JSON_NOT_WIRE'})+'\n',bytes=Buffer.byteLength(row);
    if(bytes>this.policy.maxChunkBytes){this.stats.oversizeOmitted++;if(now<=this.contextUntil)this.warn('RAW_CONTEXT_OVERSIZE');return;}
    this.buffer.push({row,bytes,time:now,sequence:event.sequence});this.bufferBytes+=bytes;
    while(this.buffer.length&&(this.bufferBytes>this.policy.maxMemoryBytes||this.buffer[0].time<now-this.policy.contextBeforeMs)){this.bufferBytes-=this.buffer.shift().bytes;}
    const channel=event.symbol+':'+event.venue+':'+event.kind+':'+(event.payload.frameKind??''),second=Math.floor(now/1000);
    const sample=this.sampleSeconds.get(channel)!==second;this.sampleSeconds.set(channel,second);
    if(this.sampleSeconds.size>64)this.sampleSeconds.delete(this.sampleSeconds.keys().next().value);
    if(this.disabled){this.stats.storageLost++;return;}
    if(now<=this.contextUntil){this.add({row,bytes,time:now,sequence:event.sequence},true);this.stats.contextEvents++;}
    else if(sample){this.add({row,bytes,time:now,sequence:event.sequence},false);this.stats.sampled++;}
    else this.stats.plannedOmitted++;
  }
  trigger(key,now,{force=false}={}){
    const previous=this.lastTriggers.get(key)??-Infinity;
    if(!force&&(now-previous<60000||now-this.lastGlobalContext<10000)){this.stats.contextsSuppressed++;return false;}
    this.lastTriggers.set(key,now);this.lastGlobalContext=now;
    if(this.lastTriggers.size>64)this.lastTriggers.delete(this.lastTriggers.keys().next().value);
    this.contextUntil=Math.max(this.contextUntil,now+this.policy.contextAfterMs);this.contextKeys.push(key);if(this.contextKeys.length>64)this.contextKeys.shift();
    if(this.disabled){this.warn('RAW_REQUIRED_CONTEXT_UNAVAILABLE');return false;}
    for(const r of this.buffer)this.add(r,true);
    return true;
  }
  add(row,context){
    if(this.disabled){this.stats.storageLost++;return;}
    if(this.chunkBytes+row.bytes>this.policy.maxChunkBytes)this.flush(row.time);
    if(this.disabled){this.stats.storageLost++;return;}
    // Duplicates in overlapping context are explicit; dedupe within current chunk.
    const duplicate=this.chunk.find(x=>x.sequence===row.sequence&&x.time===row.time);
    if(duplicate){if(context)duplicate.context=true;return;} // promotion must preserve unresolved context even when already sampled
    this.chunk.push({...row,context});this.chunkBytes+=row.bytes;
  }
  usage(){let total=0;for(const name of fs.readdirSync(this.dir)){const p=path.join(this.dir,name);if(fs.lstatSync(p).isSymbolicLink())throw new Error('RAW_SYMLINK');total+=fs.statSync(p).size;}return total;}
  prune(now,need=0){
    const db=this.history.db;
    if(!this.history.hasRisk())db.exec('UPDATE raw_segments SET protected=0 WHERE deleted_ms IS NULL AND protected<>0'); // index-only: rows already 0 are not rewritten
    // Protection is bounded: context older than max(2x retention, 1 h) is released even while risk persists (a stuck SIM slot must not pin the whole quota).
    const expired=db.prepare('UPDATE raw_segments SET protected=0 WHERE deleted_ms IS NULL AND protected=1 AND last_ms<?').run(now-Math.max(2*this.policy.retentionMs,3600000)).changes;if(expired)this.stats.protectionExpired=(this.stats.protectionExpired??0)+expired;
    let bytes=this.usage();
    for(const r of db.prepare('SELECT id,file,bytes,last_ms FROM raw_segments WHERE deleted_ms IS NULL AND protected=0 ORDER BY id').all()){ // no context_json; partial index raw_segments_live
      if(r.last_ms>=now-this.policy.retentionMs&&bytes+need<=this.policy.quotaBytes)continue;
      if(!/^raw\/segment-[a-zA-Z0-9.-]+\.jsonl\.gz$/.test(r.file))throw new Error('RAW_SEGMENT_PATH');
      const p=path.join(this.history.dir,r.file);if(fs.existsSync(p)){if(fs.lstatSync(p).isSymbolicLink())throw new Error('RAW_SYMLINK');fs.unlinkSync(p);bytes-=r.bytes;}
      db.prepare('UPDATE raw_segments SET deleted_ms=? WHERE id=?').run(now,r.id);
    }
    return bytes+need<=this.policy.quotaBytes;
  }
  flush(now){
    if(!this.chunk.length||this.disabled)return;
    try{
      const bytes=zlib.gzipSync(Buffer.from(this.chunk.map(x=>x.row).join('')),{level:1});
      if(!this.prune(now,bytes.length))throw new Error('RAW_QUOTA_PROTECTED_EVIDENCE');
      const file='raw/segment-'+Math.floor(now)+'-'+crypto.randomBytes(6).toString('hex')+'.jsonl.gz',target=path.join(this.history.dir,file),tmp=target+'.part';
      const fd=fs.openSync(tmp,'wx',0o600);try{fs.writeFileSync(fd,bytes);fs.fsyncSync(fd);}finally{fs.closeSync(fd);}fs.renameSync(tmp,target);
      const context=this.chunk.some(x=>x.context),protectedEvidence=context&&this.history.hasRisk();
      this.history.db.prepare('INSERT INTO raw_segments(run_id,file,sha256,bytes,first_ms,last_ms,event_n,coverage,protected,context_json) VALUES(?,?,?,?,?,?,?,?,?,?)').run(this.history.runId,file,C.hash(bytes),bytes.length,this.chunk[0].time,this.chunk.at(-1).time,this.chunk.length,'CHANNEL_1S_SAMPLE_AND_BOUNDED_SIGNAL_CONTEXT_NOT_FULL_REPLAY',protectedEvidence?1:0,C.stringify({keys:this.contextKeys,policy:this.policy}));
      this.stats.written+=this.chunk.length;this.stats.segments++;this.chunk=[];this.chunkBytes=0;
      if(this.warning){this.warning=null;this.onRecovered();}
    }catch(e){
      // Quota pressure drops this chunk and retries at the next flush; only I/O or integrity failures disable the store until restart.
      if(e.message!=='RAW_QUOTA_PROTECTED_EVIDENCE')this.disabled=true;
      this.stats.storageLost+=this.chunk.length;this.chunk=[];this.chunkBytes=0;this.warn(e.message);}
  }
  checkpoint(){return C.clone({buffer:this.buffer,bufferBytes:this.bufferBytes,chunk:this.chunk,chunkBytes:this.chunkBytes,sampleSeconds:this.sampleSeconds,lastTriggers:this.lastTriggers,contextUntil:this.contextUntil,contextKeys:this.contextKeys,lastGlobalContext:this.lastGlobalContext,stats:this.stats,disabled:this.disabled,warning:this.warning});}
  restore(s){const currentOrphans=this.stats.orphanFiles;Object.assign(this,C.clone(s));this.disabled=false;this.warning=null;/* a restart re-evaluates quota and I/O instead of inheriting a dead store */if(currentOrphans){this.stats.orphanFiles=currentOrphans;this.warn('RAW_ORPHAN_REQUIRES_REVIEW');}}
  status(){let bytes=null;try{bytes=this.usage();}catch(e){this.warn(e.message);}return {policy:this.policy,coverage:'1s channel samples + bounded context, NOT a second full collector or exact tick replay',stats:this.stats,bytes,quotaBytes:this.policy.quotaBytes,disabled:this.disabled,warning:this.warning,contextUntil:this.contextUntil};}
}
module.exports={RawRetention};
