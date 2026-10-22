'use strict';
const {Execution}=require('./engine.cjs');
const {createBridge}=require('./strategy.cjs');
const {canonicalMarket}=require('./live-feed.cjs');
class ObserveOnly{
  constructor(symbol){this.symbol=symbol;this.counts={market:0,quote:0,book:0,pressure:0,trade:0};this.venues={};}
  ingest(e){
    this.counts[e.kind]++;
    if(e.kind==='market'){
      this.counts[e.payload.frameKind]++;
      this.venues[e.venue]={bid:e.payload.bid,ask:e.payload.ask,lastReceiveMs:e.recv_ms,lastSourceMs:e.source_ms,
        lastBookReceiveMs:e.payload.frameKind==='book'?e.recv_ms:(this.venues[e.venue]?.lastBookReceiveMs??null),
        frameKind:e.payload.frameKind,hashKind:e.hashKind??'canonical-local-payload-NOT-raw-wire'};
    }
  }
  invalidate(){for(const v of Object.values(this.venues))v.lastBookReceiveMs=null;}
  snapshot(now){return {symbol:this.symbol,readiness:'UNCONFIGURED',mode:'observe-only',validatedNativeStrategy:false,
    btcThresholdsApplied:false,counts:this.counts,venues:Object.fromEntries(Object.entries(this.venues).map(([k,v])=>[k,{...v,receiveAgeMs:now-v.lastReceiveMs}])),simulatedIntents:0};}
}
class AssetRouter{
  constructor(){this.profiles=new Map();this.global={pressure:0,trade:0,unrouted:0};}
  register(symbol,consumer,{validatedNativeProfile=false,observeOnly=true}={}){
    if(!['BTC','ETH'].includes(symbol)||this.profiles.has(symbol)||!consumer?.ingest||!consumer?.snapshot||(!observeOnly&&!validatedNativeProfile))throw new Error('Explicit validated profile or observe-only consumer required');
    this.profiles.set(symbol,consumer);
  }
  ingest(e){const p=this.profiles.get(e.symbol);if(p)p.ingest(e);else if(e.kind==='pressure'||e.kind==='trade')this.global[e.kind]++;else this.global.unrouted++;}
  invalidate(){for(const p of this.profiles.values())p.invalidate?.();}
  snapshot(now){return {assets:Object.fromEntries([...this.profiles].map(([k,v])=>[k,v.snapshot(now)])),global:this.global};}
}
class IncrementalSession{
  constructor(config,launch,experiment,scenario,startMs,{persistentDb=null}={}){
    this.startMs=startMs;this.config=config;this.now=startMs;this.nextTick=startMs;
    this.engine=new Execution(config,experiment,scenario,startMs);
    if(persistentDb)this.engine.persistentDb=persistentDb;
    this.bridge=createBridge(this.engine,launch,experiment,startMs);this.engine.bridge=this.bridge;
    this.engine.inputHealthy=false;this.engine.integrityWarmUntil=startMs+180000;
    this.inputCounts={market:0,quote:0,book:0,pressure:0,trade:0,rejectedStale:0,rejectedFuture:0,rejectedMarket:0,missedTicks:0};
    this.lastInput=null;this.lastInvalidate=null;
    this.receiveLatency={count:0,sumMs:0,minMs:null,maxMs:null,clockSkewUnknown:true};
    this.router=new AssetRouter();this.eth=new ObserveOnly('ETH');
    this.router.register('ETH',this.eth);
    this.router.register('BTC',{ingest:e=>this.ingestBTC(e),invalidate:()=>{},snapshot:now=>this.btcSnapshot(now)},{observeOnly:false,validatedNativeProfile:true});
  }
  setContinuity(healthy){this.engine.inputHealthy=healthy;}
  invalidate(reason,t=this.now){
    // A stale/stall storm re-invalidates on every event, and each journal note is a full fsync'd safety snapshot; that made the storm
    // self-sustaining (~1 note per event). Reset and warmup still apply on every call; only the identical repeated note is coalesced
    // (and counted) until fresh input is accepted or 5 s pass.
    const prev=this.lastInvalidate,quiet=!!prev&&prev.reason===reason&&!this.acceptedSinceInvalidate&&t-(prev.notedAtMs??prev.atMs)<5000;
    this.lastInvalidate={reason,atMs:t,notedAtMs:quiet?(prev.notedAtMs??prev.atMs):t};this.acceptedSinceInvalidate=false;
    this.bridge.invalidate(t,reason,{quiet});this.router.invalidate();
  }
  advanceTo(target,{tick=true}={}){
    if(!Number.isFinite(target)||target<this.now)throw new Error('Realtime clock regression');
    target=this.engine.executionPrices?.target(target)??target;if(target<this.now)return;
    while(this.engine.timers.length&&this.engine.timers[0].at<=target){
      const timer=this.engine.timers.shift();this.engine.mark(timer.at);this.engine.fire(timer);
    }
    this.engine.mark(target);this.now=target;
    if(tick&&target>=this.nextTick){
      const skipped=Math.max(0,Math.floor((target-this.nextTick)/100));this.inputCounts.missedTicks+=skipped;
      if(skipped>=10)this.invalidate('processing_stall_no_retroactive_decisions',target);
      this.bridge.step(target);this.nextTick=target+100;
    }
  }
  ingest(e,processedAtMs){
    this.advanceTo(processedAtMs,{tick:false});
    const lag=processedAtMs-e.recv_ms;
    this.receiveLatency.count++;this.receiveLatency.sumMs+=lag;
    this.receiveLatency.minMs=this.receiveLatency.minMs===null?lag:Math.min(lag,this.receiveLatency.minMs);
    this.receiveLatency.maxMs=this.receiveLatency.maxMs===null?lag:Math.max(lag,this.receiveLatency.maxMs);
    if(lag< -1000){this.inputCounts.rejectedFuture++;this.invalidate('receive_clock_ahead',processedAtMs);return false;}
    if(lag>(this.config.realtime.maxReceiveAgeMs??2500)){this.inputCounts.rejectedStale++;this.invalidate('stale_received_event',processedAtMs);return false;}
    this.lastInput={sequence:e.sequence,kind:e.kind,symbol:e.symbol,venue:e.venue,recv_ms:e.recv_ms,source_ms:e.source_ms,processedAtMs};this.acceptedSinceInvalidate=true;
    this.router.ingest(e);return true;
  }
  ingestBTC(e){
    this.inputCounts[e.kind]++;
    if(e.kind==='market'){
      this.inputCounts[e.payload.frameKind]++;
      if(this.bridge.market(canonicalMarket(e))!==true){this.inputCounts.rejectedMarket++;this.invalidate('invalid_market_or_source_clock',this.now);}
    }
    // Public pressure/trades are observations only; no pairing to invented own intents.
  }
  btcSnapshot(now){
    const elapsed=now>=Math.max(this.startMs+180000,this.engine.integrityWarmUntil),f=this.bridge.api.feed;
    const currentDepth=['hl','bf'].every(v=>f[v].bids.length&&f[v].asks.length&&now-f[v].bookT<=(v==='hl'?10000:3000)&&now-f[v].t<=this.bridge.api.cfg.maxFeedAgeMs);
    return {symbol:'BTC',profile:'reviewed-vchera2-baseline',mode:'simulated',coldStart:true,
      marketInputPolicy:this.bridge.marketPolicy,
      missingProductionHistory:true,warmup:{initialUntilMs:this.startMs+180000,recoveryUntilMs:this.engine.integrityWarmUntil,
        elapsed,complete:elapsed&&currentDepth&&this.engine.inputHealthy&&(this.engine.tickStats?.entryEligibleInputTicks??0)>0},cutoff:this.bridge.cutoff,
      inputCounts:this.inputCounts,inputHealthy:this.engine.inputHealthy,lastInvalidate:this.lastInvalidate,
      signalAndGateCounts:this.engine.tickStats??null,lastCandidate:this.engine.lastCandidate??null,
      simulated:this.engine.summary()};
  }
  snapshot(now=this.now){
    const r=this.receiveLatency;
    return {...this.router.snapshot(now),lastInput:this.lastInput,
      latency:{inputMeasured:{receiveToProcessCount:r.count,receiveToProcessMeanMs:r.count?r.sumMs/r.count:null,
        receiveToProcessMinMs:r.minMs,receiveToProcessMaxMs:r.maxMs,clockSkewUnknown:true},
        executionAssumed:{scenario:this.engine.scenario.id,class:'uncalibrated scenario; not measured submit-to-fill',rank:null}},
      financialPerformance:'simulated; NOT observed cash payout'};
  }
  close(){this.bridge.close();}
}
module.exports={ObserveOnly,AssetRouter,IncrementalSession};
