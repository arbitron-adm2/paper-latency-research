'use strict';
const {sha}=require('../../apps/papertrade-shadow/strategy.cjs');
const START=Date.parse('2026-10-20T08:30:00Z');
function event(t,venue,payload,sequence=0,quality={}){
  return {schema:1,event_ms:t,recv_ms:t,source:venue,sequence,market:'BTC',account:null,kind:'market',venue,payload,
    raw_sha256:sha(JSON.stringify({t,venue,payload,sequence})),quality:{synthetic:true,...quality}};
}
function tape({end=195000,depth=true,gone=181000,adverse=186000}={}){
  const result=[];
  for(let i=0;i<=end;i+=500){
    const px=i>=adverse?81828:81800;
    for(const venue of ['hl','bf']){
      const quote={bid:String(px-0.5),ask:String(px+0.5),frameKind:'quote'};
      result.push(event(START+i,venue,quote,result.length));
      if(depth){const book={...quote,frameKind:'book',bids:[[quote.bid,'1']],asks:[[quote.ask,i<gone?'12':'1']]};
        result.push(event(START+i,venue,book,result.length));}
    }
  }
  return result;
}
module.exports={START,event,tape};
