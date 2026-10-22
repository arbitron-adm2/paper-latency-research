'use strict';
// Capture the EXISTING canonical loopback feed into the ONE research database.
// No exchange/credential/signer imports; bounded buffers and explicit gaps.
const path=require('node:path'),fs=require('node:fs');const {TapeStore}=require('./tape-store.cjs');const {mirrorShadow}=require('./mirror.cjs');const {LoopbackReader}=require('../papertrade-shadow/live-feed.cjs');
const ROOT=path.resolve(__dirname,'../..'),DB=path.join(ROOT,'data/unified/market.sqlite3');
async function record({stopAfterMs=Infinity}={}){
 const store=new TapeStore(DB),reader=new LoopbackReader({limit:256}),started=Date.now();let lastMirror=0,cursor=0,boot=null,rows=[],bytes=0,lastFlush=started,stopping=false,errors=0,lastError=null;
 const finish=()=>{stopping=true;reader.close();};process.once('SIGTERM',finish);process.once('SIGINT',finish);
 const flush=()=>{if(rows.length){store.append(rows,boot);rows=[];bytes=0;lastFlush=Date.now();}};
 const sleep=ms=>new Promise(r=>setTimeout(r,ms));
 try{while(!stopping&&Date.now()-started<stopAfterMs){
  try{
   const b=await reader.get(cursor),now=Date.now();
   if(boot!==b.bootId){flush();store.gap(boot?'SOURCE_BOOT_CHANGED':'CAPTURE_STARTED',{previousBoot:boot,bootId:b.bootId,firstSequence:b.firstSequence,lastSequence:b.lastSequence});boot=b.bootId;const committed=store.db.prepare('SELECT MAX(last_sequence) n FROM strategy_tape_chunks WHERE boot_id=?').get(boot).n??0;cursor=Math.max(b.firstSequence-1,committed);}
   if(b.gap&&cursor<b.firstSequence-1){flush();store.gap('RING_GAP',{after:cursor,firstSequence:b.firstSequence,lastSequence:b.lastSequence});cursor=b.firstSequence-1;}
   for(const e of b.events){if(e.sequence<=cursor)continue;if(e.sequence!==cursor+1){flush();store.gap('SEQUENCE_GAP',{after:cursor,observed:e.sequence});}rows.push(e);bytes+=Buffer.byteLength(JSON.stringify(e));cursor=e.sequence;}
   if(rows.length&&(now-lastFlush>=1000||bytes>=2*1024*1024))flush();
   if(bytes>16*1024*1024)throw new Error('CAPTURE_MEMORY_BOUND');
   if(fs.statfsSync(path.dirname(DB)).bavail*fs.statfsSync(path.dirname(DB)).bsize<5*1024**3)throw new Error('DISK_RESERVE_STOP');
   if(now-lastMirror>=5000){mirrorShadow(store.db,path.join(ROOT,'reports/persistent-shadow-state'));lastMirror=now;}
   lastError=null;await sleep(b.events.length>=256&&cursor<b.lastSequence?1:40);
  }catch(e){if(stopping)break;errors++;lastError=e.message;if(errors<=3||errors%30===0)process.stderr.write(JSON.stringify({error:lastError,events:store.events,bufferBytes:bytes})+'\n');if(bytes>16*1024*1024||lastError==='DISK_RESERVE_STOP')throw e;await sleep(200);}
 }
 flush();process.stdout.write(JSON.stringify({status:'STOPPED',sessionId:store.id,events:store.events,chunks:store.chunks,errors,lastError})+'\n');
 }finally{reader.close();store.close();process.removeListener('SIGTERM',finish);process.removeListener('SIGINT',finish);}
}
module.exports={record,DB};if(require.main===module)record().catch(e=>{process.stderr.write(e.stack+'\n');process.exitCode=1;});
