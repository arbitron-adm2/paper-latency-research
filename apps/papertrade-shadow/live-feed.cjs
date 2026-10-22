'use strict';
const {sha}=require('./strategy.cjs');
const ENDPOINT='http://127.0.0.1:18762/api/research/events';
const stable=x=>Array.isArray(x)?x.map(stable):x&&typeof x==='object'?Object.fromEntries(Object.keys(x).sort().map(k=>[k,stable(x[k])])):x;
function validateSource(url){if(url!==ENDPOINT)throw new Error('REALTIME_SOURCE_DENIED: exact fixed loopback GET only');return url;}
function integer(x,min=0,max=Number.MAX_SAFE_INTEGER){return Number.isSafeInteger(x)&&x>=min&&x<=max;}
function timestamp(x){return Number.isFinite(x)&&x>=0&&x<=Number.MAX_SAFE_INTEGER;}
function quoteNumber(x,positive=true){return typeof x==='string'&&x.length<=40&&/^\d+(\.\d+)?$/.test(x)&&Number.isFinite(Number(x))&&(positive?Number(x)>0:Number(x)>=0);}
function validateEvent(e){
  if(!e||e.schemaVersion!==1||!integer(e.sequence,1)||!timestamp(e.recv_ms)||!(e.source_ms===null||timestamp(e.source_ms))||!['market','pressure','trade'].includes(e.kind)||!['BTC','ETH',null].includes(e.symbol)||!['hl','bf','bs','paper'].includes(e.venue)||!e.payload||typeof e.payload!=='object'||Array.isArray(e.payload)||typeof e.provenance!=='string'||!e.provenance.length||e.provenance.length>2048||!/^([a-f0-9]{64})$/.test(e.payload_sha256))throw new Error('Invalid contract event');
  const encoded=JSON.stringify(e.payload),a=sha(encoded),b=sha(JSON.stringify(stable(e.payload)));
  if(e.payload_sha256!==a&&e.payload_sha256!==b)throw new Error('Local payload checksum mismatch (not a wire hash)');
  if(e.kind==='market'){
    const p=e.payload;
    if(!['BTC','ETH'].includes(e.symbol)||!['hl','bf','bs'].includes(e.venue)||!quoteNumber(p.bid)||!quoteNumber(p.ask)||Number(p.ask)<Number(p.bid)||!['quote','book'].includes(p.frameKind)||p.instrumentId!==(e.symbol==='BTC'?0:1)||String(p.priceScale)!==(e.symbol==='BTC'?'10':'100')||typeof p.sourceTimestampKnown!=='boolean'||p.sourceTimestampKnown!==(e.source_ms!==null))throw new Error('Invalid market instrument/quote metadata');
    const hasB=Object.hasOwn(p,'bids'),hasA=Object.hasOwn(p,'asks');
    if(p.frameKind==='quote'&&(hasB||hasA))throw new Error('Quote frame cannot invent depth');
    if(p.frameKind==='book'){
      if(!hasB||!hasA||![p.bids,p.asks].every(xs=>Array.isArray(xs)&&xs.length>0&&xs.length<=100&&xs.every(x=>Array.isArray(x)&&x.length===2&&quoteNumber(x[0])&&quoteNumber(x[1],false))))throw new Error('Invalid observed depth');
      if(Number(p.bids[0][0])!==Number(p.bid)||Number(p.asks[0][0])!==Number(p.ask))throw new Error('Book top/quote disagreement');
    }
    // Trust the explicit venue in the reviewed loopback contract, NEVER the ps
    // field or relative prices. If provenance names an upstream URL, cross-check it.
    const urls=e.provenance.match(/wss?:\/\/[^\s"']+/g)||[];
    for(const u of urls){let h;try{h=new URL(u).hostname;}catch{throw new Error('Invalid provenance URL');}
      if(h==='fstream.binance.com'&&e.venue!=='bf')throw new Error('Venue/provenance conflict');
      if(['stream.binance.com','data-stream.binance.vision'].includes(h)&&e.venue!=='bs')throw new Error('Venue/provenance conflict');
      if(h==='api.hyperliquid.xyz'&&e.venue!=='hl')throw new Error('Venue/provenance conflict');
      if(h==='dstream.binance.com')throw new Error('Coin-M is not USD-M bf');
    }
  }
  return {...e,hashConvention:e.payload_sha256===a?'JSON.stringify(payload)':'sorted-key local JSON',hashKind:'canonical-local-payload-NOT-raw-wire'};
}
function validateEnvelope(x,limit){
  if(!x||x.schemaVersion!==1||x.mode!=='READ_ONLY_RESEARCH_NO_ORDERS'||typeof x.bootId!=='string'||!x.bootId.length||x.bootId.length>256||!integer(x.observedAtMs)||!integer(x.firstSequence)||!integer(x.lastSequence)||x.lastSequence<x.firstSequence-1||typeof x.gap!=='boolean'||!Array.isArray(x.events)||x.events.length>limit)throw new Error('Invalid feed envelope');
  let previous=0;
  for(const e of x.events){validateEvent(e);if(e.sequence<=previous||e.sequence<x.firstSequence||e.sequence>x.lastSequence)throw new Error('Invalid ordered response sequence');previous=e.sequence;}
  return x;
}
class LoopbackReader{
  constructor({source=ENDPOINT,limit=128,timeoutMs=2000,maxBytes=2097152}={}){
    validateSource(source);if(!integer(limit,1,256)||!integer(timeoutMs,100,5000)||!integer(maxBytes,1024,4194304))throw new Error('Invalid bounded GET options');
    Object.assign(this,{source,limit,timeoutMs,maxBytes,requests:0});
  }
  get(after){
    if(!integer(after))return Promise.reject(new Error('Invalid cursor'));
    const http=require('node:http'); // lazy: offline paths never import a transport
    this.requests++;
    const owner=this;
    return new Promise((resolve,reject)=>{
      let bytes=0,done=false;const chunks=[];
      const req=http.request({hostname:'127.0.0.1',port:18762,path:'/api/research/events?after='+after+'&limit='+this.limit,method:'GET',agent:false,
        headers:{Accept:'application/json','User-Agent':'PaperShadowIncremental/1'}},res=>{
        if(res.statusCode!==200){res.resume();req.destroy(new Error('GET status '+res.statusCode+'; redirects/retries not followed'));return;}
        if(!String(res.headers['content-type']||'').includes('application/json')){req.destroy(new Error('Expected JSON'));return;}
        res.on('data',b=>{bytes+=b.length;if(bytes>this.maxBytes){req.destroy(new Error('Response byte bound exceeded'));return;}chunks.push(b);});
        res.on('error',finish);
        res.on('end',()=>{try{finish(null,validateEnvelope(JSON.parse(Buffer.concat(chunks).toString('utf8')),this.limit));}catch(e){finish(e);}});
      });
      this.activeRequest=req;
      const timer=setTimeout(()=>req.destroy(new Error('Bounded loopback GET timeout')),this.timeoutMs);
      function finish(error,value){if(done)return;done=true;clearTimeout(timer);if(owner.activeRequest===req)owner.activeRequest=null;error?reject(error):resolve(value);}
      req.on('error',finish);req.end();
    });
  }
  close(){const r=this.activeRequest;this.activeRequest=null;r?.destroy(new Error('LOOPBACK_READER_STOPPED'));}
}
class Continuity{
  constructor(){this.bootId=null;this.cursor=0;this.lastReceiveMs=null;this.contiguousBatches=0;this.counts={boots:0,gaps:0,duplicates:0,bootstrapSkipped:0,events:0};}
  consume(response){
    validateEnvelope(response,256);
    const notices=[],accepted=[];let invalidated=false;
    if(this.bootId===null){this.bootId=response.bootId;this.cursor=response.lastSequence;this.counts.boots++;this.counts.bootstrapSkipped=response.lastSequence?response.lastSequence-response.firstSequence+1:0;
      notices.push({kind:'bootstrap_latest',bootId:this.bootId,skippedFrom:response.firstSequence,skippedThrough:response.lastSequence});
      return {events:[],notices,invalidated:true,continuous:false};
    }
    if(this.bootId!==response.bootId||response.gap){
      const reason=this.bootId!==response.bootId?'boot_changed':'history_gap';
      if(this.bootId!==response.bootId)this.counts.boots++;
      this.bootId=response.bootId;this.cursor=response.lastSequence;this.lastReceiveMs=null;this.contiguousBatches=0;this.counts.gaps++;
      notices.push({kind:reason,bootId:this.bootId,skippedThrough:response.lastSequence});
      return {events:[],notices,invalidated:true,continuous:false};
    }
    for(const raw of response.events){
      const e=validateEvent(raw);
      if(e.sequence<=this.cursor){this.counts.duplicates++;notices.push({kind:'duplicate_sequence',sequence:e.sequence});continue;}
      if(e.sequence!==this.cursor+1||(this.lastReceiveMs!==null&&e.recv_ms<this.lastReceiveMs)){
        invalidated=true;this.counts.gaps++;notices.push({kind:'sequence_or_receive_regression',expected:this.cursor+1,observed:e.sequence});
        this.cursor=response.lastSequence;this.lastReceiveMs=null;accepted.length=0;break;
      }
      this.cursor=e.sequence;this.lastReceiveMs=e.recv_ms;accepted.push(e);this.counts.events++;
    }
    if(invalidated)this.contiguousBatches=0;else this.contiguousBatches++;
    return {events:accepted,notices,invalidated,continuous:this.contiguousBatches>=2};
  }
}
function canonicalMarket(e){
  validateEvent(e);
  return {schema:1,event_ms:e.source_ms??e.recv_ms,recv_ms:e.recv_ms,sequence:e.sequence,kind:'market',market:e.symbol,venue:e.venue,
    source:'dashboard-contract:'+e.venue,payload:e.payload,payload_sha256:e.payload_sha256,
    quality:{timestamp:e.source_ms===null?'receive-only':'source',depth:e.payload.frameKind==='book'?'observed':'missing',
      provenance:e.provenance,venueAttribution:'explicit reviewed loopback contract; ps never used',hashKind:'canonical-local-payload-NOT-raw-wire'}};
}
module.exports={ENDPOINT,validateSource,validateEvent,validateEnvelope,LoopbackReader,Continuity,canonicalMarket};
