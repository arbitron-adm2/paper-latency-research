'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const {ResearchFeed,sourceTime}=require('../../apps/vchera2/research-feed.cjs');
const {MarketData}=require('../../apps/vchera2/market-data.cjs');
test('research broker contains real observed market depth with provenance, no fake wire hash',()=>{
 const market=new MarketData({now:()=>10000}),broker=new ResearchFeed({now:()=>10000}).attach(market);
 market.hyperliquid({channel:'l2Book',data:{coin:'ETH',time:9999,levels:[[{px:'2300.11',sz:'2'}],[{px:'2300.12',sz:'3'}]]}});
 const r=broker.read();assert.equal(r.events.length,1);const e=r.events[0];assert.equal(e.symbol,'ETH');assert.equal(e.payload.frameKind,'book');assert.equal(e.payload.instrumentId,1);assert.equal(e.payload.bids[0][0],'2300.11');assert.equal(e.source_ms,9999);assert.match(e.payload_sha256,/^[0-9a-f]{64}$/);assert.equal(e.raw_sha256,undefined);assert.equal(r.gap,false);
});
test('quote observer and unknown/rejected packets never fabricate depth or corrupt prices',()=>{
 const market=new MarketData({now:()=>10000}),broker=new ResearchFeed({now:()=>10000}).attach(market);
 market.onEvent(()=>{throw new Error('Observer failure must not break feeds');});
 market.binance('bf',{s:'BTCUSDT',e:'bookTicker',b:'81000',a:'81000.1',T:10000});
 market.binance('bf',{s:'BTCUSDT',e:'bookTicker',b:'81001',a:'81000'});
 const r=broker.read();assert.equal(r.events.length,1);assert.equal(r.events[0].payload.bids,undefined);assert.equal(market.snapshot().BTC.feeds.bf.mid,81000.05);
});
test('cursor batch nextAfter never skips unreturned events; restart and ring loss are explicit',()=>{
 const broker=new ResearchFeed({now:()=>10000,maxEvents:3});
 for(let i=0;i<5;i++)broker.publish({kind:'pressure',recv_ms:10000+i,payload:{snapshot:{ts:10000+i}}});
 const first=broker.read(0,1);assert.equal(first.gap,true);assert.equal(first.firstSequence,3);assert.equal(first.nextAfter,3);assert.equal(first.lastSequence,5);
 const next=broker.read(first.nextAfter,1);assert.equal(next.gap,false);assert.equal(next.events[0].sequence,4);assert.equal(next.nextAfter,4);
 const other=new ResearchFeed();assert.notEqual(other.bootId,broker.bootId);assert.equal(other.read(5).gap,true);
});
test('oversized public payload is rejected with visible cursor discontinuity and memory remains bounded',()=>{
 const broker=new ResearchFeed({maxBytes:1000});assert.equal(broker.publish({kind:'trade',recv_ms:10000,payload:{data:'x'.repeat(2000)}}),false);
 const r=broker.read();assert.equal(r.gap,true);assert.equal(r.status.rejectedOversized,1);assert.equal(r.status.retainedBytes,0);
});
test('public pressure and trades retain native evidence; missing instrument/time is unknown, not BTC or a made-up latency',()=>{
 const broker=new ResearchFeed({now:()=>1800000000000});
 broker.observePublic({channel:'pressure:current',recv_ms:1800000000000,payload:{snapshot:{ts:1799999999999,totalPendingGas:42}}});
 broker.observePublic({channel:'public:trades',recv_ms:1800000000000,payload:[{instrumentId:1,kind:'open',positionId:'42',timestampMs:1799999999999},{kind:'close',positionId:'43'}]});
 const r=broker.read();assert.equal(r.events.length,3);assert.equal(r.events[0].kind,'pressure');assert.equal(r.events[1].symbol,'ETH');assert.equal(r.events[2].symbol,null);assert.equal(r.events[2].source_ms,null);assert.equal(sourceTime({timestamp:1700000000}),null);assert.equal(r.status.tradingMutations,0);
});
test('feed returns immutable copies and rejects cursor abuse',()=>{
 const b=new ResearchFeed();b.publish({kind:'pressure',recv_ms:10000,payload:{n:1}});b.read().events[0].payload.n=999;assert.equal(b.read().events[0].payload.n,1);
 for(const [after,limit]of[[-1,1],[1.2,1],[0,0],[0,257],[Number.MAX_SAFE_INTEGER+1,1]])assert.throws(()=>b.read(after,limit));
});
