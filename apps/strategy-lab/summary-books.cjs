'use strict';
// Explicit research policy. These objects are never quotes and never enter execution prices.
class SummaryBooks {
 constructor({policy='strict',maxAgeMs=1500}={}){if(!['strict','bf_l1_proxy'].includes(policy))throw new Error('UNKNOWN_SUMMARY_POLICY');this.policy=policy;this.maxAgeMs=maxAgeMs;this.summary=null;this.top=new Map();this.feed=null;this.stats={summarySeen:0,topSeen:0,summaryLookups:0,proxyLookups:0,missing:0};}
 observe(e){
  if(e.symbol!=='BTC')return;
  if(e.kind==='book_summary'&&e.venue==='hl'){this.summary={...e.payload,at:e.recv_ms,sourceRecordId:e.sourceRecordId,sourceId:e.sourceId};this.stats.summarySeen++;return;}
  if(e.kind==='market'&&e.payload.frameKind==='quote'&&['bf','bs'].includes(e.venue)){
   const p=e.payload,b=Number(p.bid),a=Number(p.ask),bq=Number(p.bidSize),aq=Number(p.askSize);
   this.top.set(e.venue,{at:Math.min(e.source_ms??e.recv_ms,e.recv_ms),bid:b,ask:a,bidUsd:b*bq,askUsd:a*aq});this.stats.topSeen++;
  }
 }
 at(v,t){
  const f=this.feed?.[v];if(f?.bids.length&&f?.asks.length&&t-f.bookT<=(v==='hl'?10000:3000))return null;
  if(v==='hl'){
   const s=this.summary;if(!s||t<s.at||t-s.at>this.maxAgeMs||s.imbalancePct==null||s.bidNearUsd==null||s.askNearUsd==null){this.stats.missing++;return null;}
   this.stats.summaryLookups++;return{imb:s.imbalancePct/100,l1b:s.bidNearUsd,l1a:s.askNearUsd,researchKind:'Vlad exported summary; band/rounding/source age not independently verified',wall:s.wallUsd==null?null:{usd:s.wallUsd,side:s.wallSide,dist:s.wallDistanceUsd}};
  }
  const q=this.top.get(v);
  if(this.policy!=='bf_l1_proxy'||!q||t<q.at||t-q.at>this.maxAgeMs||![q.bidUsd,q.askUsd].every(x=>Number.isFinite(x)&&x>=0)||q.bidUsd+q.askUsd<=0){this.stats.missing++;return null;}
  this.stats.proxyLookups++;return{imb:(q.bidUsd-q.askUsd)/(q.bidUsd+q.askUsd),l1b:q.bidUsd,l1a:q.askUsd,researchKind:'Best-level imbalance proxy; not native +/-0.02% depth',wall:null,top:q};
 }
}
module.exports={SummaryBooks};
