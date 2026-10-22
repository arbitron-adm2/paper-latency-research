'use strict';
// Execution evidence is separate from the strategy's last quote. A whole HTTP
// batch may be staged, but lookup is strictly past-only at the execution time.
const POLICY='causal-hl-bbo-v1';
class ExecutionPrices{
  constructor(asset='BTC'){this.asset=asset;this.rows=[];this.head=0;this.lastSequence=0;this.watermarkMs=Infinity;}
  observe(e){
    if(e.kind!=='market'||e.venue!=='hl'||(e.symbol??e.market)!==this.asset)return;
    const p=e.payload,recv=Number(e.recv_ms),source=e.source_ms??e.event_ms??recv;
    if(p.frameKind!=='quote'||!Number.isFinite(recv)||!Number.isFinite(source)||source>recv+1000)return;
    const bid=Number(p.bid),ask=Number(p.ask),sequence=e.sequence??0;
    if(!(bid>0&&ask>=bid)||!Number.isFinite(ask))return;
    const last=this.rows.at(-1);
    if(last&&(recv<last.recvMs||(sequence&&sequence<=this.lastSequence)))return;
    const row={bid,ask,recvMs:recv,sourceMs:source,sequence,sourceTimestampKnown:p.sourceTimestampKnown??e.source_ms!=null};
    this.rows.push(row);this.lastSequence=sequence;
    while(this.head<this.rows.length-1&&(this.rows[this.head].recvMs<recv-120000||this.rows.length-this.head>8192))this.head++;
    if(this.head>1024){this.rows=this.rows.slice(this.head);this.head=0;}
  }
  stage(events,watermarkMs){for(const e of events)this.observe(e);if(Number.isFinite(watermarkMs))this.watermarkMs=this.watermarkMs===Infinity?watermarkMs:Math.max(this.watermarkMs,watermarkMs);}
  reset(watermarkMs){this.rows=[];this.head=0;this.lastSequence=0;this.watermarkMs=watermarkMs;}
  target(t){return Math.min(t,this.watermarkMs);}
  at(t,{maxAgeMs=1500,maxSpread=6}={}){
    let lo=this.head,hi=this.rows.length;
    while(lo<hi){const mid=(lo+hi)>>>1;if(this.rows[mid].recvMs<=t)lo=mid+1;else hi=mid;}
    const q=this.rows[lo-1];if(!q||lo-1<this.head)return null;
    const ageMs=t-Math.min(q.sourceMs,q.recvMs);
    if(ageMs<0||ageMs>maxAgeMs||q.ask-q.bid>maxSpread)return null;
    return {...q,ageMs,executionAtMs:t,policy:POLICY,priceModel:'HL midpoint proxy, not a verified Paper fill'};
  }
}
module.exports={ExecutionPrices,POLICY};
