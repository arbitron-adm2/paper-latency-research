'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const {Execution}=require('../../apps/papertrade-shadow/engine.cjs');
const {createBridge,sha}=require('../../apps/papertrade-shadow/strategy.cjs');
const {IncrementalSession}=require('../../apps/papertrade-shadow/incremental.cjs');
const {Continuity,validateEvent}=require('../../apps/papertrade-shadow/live-feed.cjs');
const launch=require('../../apps/papertrade-shadow/baseline.json');
const base=require('../../apps/papertrade-shadow/scenarios.json');
const START=Date.parse('2026-10-20T08:30:00Z');
const config={...base,realtime:{maxReceiveAgeMs:2500},entryDeadline:{signalTtlMs:15000,safetySeconds:1}};
const scenario={...base.scenarios[1],entryMs:[4000],exitMs:[9000],cancelMs:[500],congestion:0,volatility:0};
function makeEngine(start=10000){const e=new Execution(config,base.experiments[0],scenario,start);e.bridge=createBridge(e,launch,base.experiments[0],start);return e;}
function frame(venue,time,{book=false,bid='81000',ask='81000.1',sourceTime=time,askSize='1'}={}){
  const p={frameKind:book?'book':'quote',bid,ask};if(book){p.bids=[[bid,'1']];p.asks=[[ask,askSize]];}
  return {schema:1,market:'BTC',venue,source:'r2-synthetic',event_ms:sourceTime,recv_ms:time,payload:p};
}
function quoteState(f){return {bid:f.bid,ask:f.ask,quoteTime:f.t,sourceTime:f.sourceTime,quoteSourceTime:f.quoteSourceTime,midOk:f.midOk,midOkT:f.midOkT};}
function live(seq,time,venue,{book=false,askSize='1'}={}){
  const p={bid:'81799.5',ask:'81800.5',frameKind:book?'book':'quote',instrumentId:0,priceScale:'10',sourceTimestampKnown:true};
  if(book){p.bids=[[p.bid,'1']];p.asks=[[p.ask,askSize]];}
  return validateEvent({schemaVersion:1,sequence:seq,kind:'market',symbol:'BTC',venue,recv_ms:time,source_ms:time,payload:p,
    payload_sha256:sha(JSON.stringify(p)),provenance:'r2 synthetic probe, not performance'});
}
function envelope(events,{first,last,boot='existing-boot',gap=false,t=START}={}){
  return {schemaVersion:1,mode:'READ_ONLY_RESEARCH_NO_ORDERS',bootId:boot,observedAtMs:t,firstSequence:first,lastSequence:last,gap,events};
}

test('r2 independent reproduction: newer HL/spot/BF book preserves every BBO field and age',t=>{
  const e=makeEngine(),cases=[];try{
    for(const venue of ['hl','bs','bf']){
      e.bridge.market(frame(venue,10000));const before=quoteState(e.bridge.api.feed[venue]);
      e.bridge.market(frame(venue,12000,{book:true,bid:'81001',ask:'81001.1'}));
      const f=e.bridge.api.feed[venue],after=quoteState(f);
      assert.deepEqual(after,before);assert.equal(f.bookT,12000);assert.equal(f.bookSourceTime,12000);assert.equal(f.bids[0][0],81001);
      cases.push({venue,before,after,bookTime:f.bookT,bookTop:f.bids[0][0],accepted:true});
    }
    assert.equal(e.bridge.marketPolicy.depthDerivedQuote,false);
    t.diagnostic('R2_PARITY '+JSON.stringify({accepted:true,check:'BOOK_FRAME_MUST_NOT_REFRESH_BBO',cases,policy:e.bridge.marketPolicy,syntheticNotPerformance:true,financialMutations:0,networkRequests:0}));
  }finally{e.bridge.close();}
});
for(const venue of ['hl','bs','bf'])test('r2 independent source ordering and stale BBO age for '+venue,()=>{
  const e=makeEngine();try{
    e.bridge.market(frame(venue,10000));e.bridge.market(frame(venue,12000,{book:true,bid:'81001',ask:'81001.1'}));
    const f=e.bridge.api.feed[venue];assert.equal(12000-f.t,2000);assert.ok(12000-f.t>e.bridge.api.cfg.maxFeedAgeMs);
    // Source quote older than most recent book, but newer than last quote: accept it.
    e.bridge.market(frame(venue,12500,{sourceTime:11000,bid:'81000.5',ask:'81000.6'}));
    assert.equal(f.t,11000);assert.equal(f.bid,81000.5);assert.equal(f.bookT,12000);
    const q=quoteState(f);
    // Older book ignored by its own clock, newer book applied; neither changes BBO.
    e.bridge.market(frame(venue,12600,{book:true,sourceTime:11500,bid:'81002',ask:'81002.1'}));
    assert.equal(f.bookT,12000);assert.deepEqual(quoteState(f),q);
    e.bridge.market(frame(venue,13000,{book:true,bid:'81003',ask:'81003.1'}));
    assert.equal(f.bookT,13000);assert.deepEqual(quoteState(f),q);
    // Old quote ignored even though its own receive time is fresh; no depth refresh.
    e.bridge.market(frame(venue,13100,{sourceTime:10500,bid:'80900',ask:'80900.1'}));
    assert.deepEqual(quoteState(f),q);assert.equal(f.bookT,13000);
  }finally{e.bridge.close();}
});
test('r2 book-only startup does not synthesize BBO, midpoint or executable price',()=>{
  const e=makeEngine();try{
    for(const venue of ['hl','bs','bf']){
      const before=quoteState(e.bridge.api.feed[venue]);e.bridge.market(frame(venue,12000,{book:true}));const f=e.bridge.api.feed[venue];
      assert.deepEqual(quoteState(f),before);assert.ok(!Number.isFinite(f.bid));assert.ok(!Number.isFinite(f.ask));assert.equal(f.t,0);assert.equal(f.bookT,12000);
    }
    e.now=12000;assert.equal(e.quote(),null);
  }finally{e.bridge.close();}
});
for(const staleVenue of ['hl','bf'])test('r2 post-warmup fresh depth cannot admit an entry on stale mandatory '+staleVenue+' BBO',()=>{
  const s=new IncrementalSession(config,launch,base.experiments[0],scenario,START);let seq=0;
  try{
    s.setContinuity(true);
    // Balanced depth + unchanged spread produce no entries while original warmup runs.
    for(let dt=0;dt<=181000;dt+=100){
      if(dt%500===0)for(const venue of ['hl','bf'])for(const book of [false,true])s.ingest(live(++seq,START+dt,venue,{book}),START+dt);
      s.advanceTo(START+dt);
    }
    assert.ok(s.engine.tickStats.entryEligibleInputTicks>0);assert.equal(s.engine.counts.submitted,0);
    const at=s.now,maxAge=s.bridge.api.cfg.maxFeedAgeMs,before=quoteState(s.bridge.api.feed[staleVenue]);
    for(let dt=100;dt<=maxAge+1400;dt+=100){
      const now=at+dt,askSize=dt>maxAge+200?'12':'1';
      for(const venue of ['hl','bf']){
        if(venue!==staleVenue)s.ingest(live(++seq,now,venue),now);
        s.ingest(live(++seq,now,venue,{book:true,askSize}),now);
      }
      s.advanceTo(now);
    }
    assert.deepEqual(quoteState(s.bridge.api.feed[staleVenue]),before);
    assert.ok(s.now-s.bridge.api.feed[staleVenue].t>maxAge);
    assert.equal(s.bridge.api.feed[staleVenue].bookT,s.now);
    assert.ok(s.engine.tickStats.reasons.stale>0);assert.ok(s.engine.tickStats.rawSignalTicks>0);
    assert.equal(s.engine.counts.submitted,0);assert.equal(s.engine.counts.filled,0);
  }finally{s.close();}
});
test('r2 new position-free attach to old bounded ring skips tail explicitly, recovers continuity, and later gaps retain risk',t=>{
  const c=new Continuity(),s=new IncrementalSession(config,launch,base.experiments[0],scenario,START);
  try{
    assert.equal(s.engine.positions.size,0);assert.equal(s.engine.reserved,0n);
    const first=c.consume(envelope([live(600001,START-1000,'hl')],{first:600001,last:606000,gap:true}));
    assert.equal(first.events.length,0);assert.equal(c.cursor,606000);assert.equal(c.counts.events,0);assert.equal(c.counts.gaps,0);
    assert.equal(first.notices[0].kind,'bootstrap_latest');assert.equal(first.notices[0].skippedThrough,606000);
    s.invalidate('bootstrap_latest',START);const bootstrapWarmup=s.engine.integrityWarmUntil;
    for(let i=1;i<=2;i++){
      const now=START+i*100,b=c.consume(envelope([live(606000+i,now,i===1?'hl':'bf')],{first:600001+i,last:606000+i,t:now}));
      assert.equal(b.invalidated,false);s.setContinuity(b.continuous);for(const e of b.events)s.ingest(e,now);s.advanceTo(now);
    }
    assert.equal(c.contiguousBatches,2);assert.equal(s.engine.inputHealthy,true);assert.equal(s.engine.integrityWarmUntil,bootstrapWarmup);
    const afterAttach={cursor:c.cursor,events:c.counts.events,gaps:c.counts.gaps,continuous:s.engine.inputHealthy};
    const q={strategy:'book',side:'S',margin:2100,ctx:{sig:.8,dev:0,spread:0,baseline:0,books:[]}};q.intentId=s.engine.submit(q);s.bridge.api.pending.push(q);
    const reservation=s.engine.reserved;assert.ok(reservation>0n);
    const gap=c.consume(envelope([live(700001,START+300,'hl')],{first:700001,last:706000,gap:true,t:START+300}));
    assert.equal(gap.invalidated,true);s.advanceTo(START+300,{tick:false});s.invalidate('history_gap',s.now);s.setContinuity(gap.continuous);
    assert.equal(s.engine.reserved,reservation);assert.equal(s.engine.intents.get(q.intentId).state,'queued');assert.equal(s.engine.inputHealthy,false);
    const restart=c.consume(envelope([live(1,START+400,'hl')],{first:1,last:10,boot:'new-boot',t:START+400}));
    assert.equal(restart.invalidated,true);s.advanceTo(START+400,{tick:false});s.invalidate('boot_changed',s.now);
    assert.equal(s.engine.reserved,reservation);assert.ok(s.bridge.api.pending.includes(q));assert.equal(c.counts.gaps,2);
    t.diagnostic('R2_BOOTSTRAP '+JSON.stringify({accepted:true,initialFirstSequence:600001,initialTailCursor:606000,
      bootstrapNotice:first.notices[0],historyDeclaredRead:false,afterAttach,subsequentRealGaps:c.counts.gaps,
      riskBeforeRaw:reservation.toString(),riskAfterRaw:s.engine.reserved.toString(),continuityImplementationChanged:false,syntheticNotPerformance:true,financialMutations:0}));
  }finally{s.close();}
});
