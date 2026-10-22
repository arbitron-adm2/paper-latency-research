'use strict';
process.env.TZ='Europe/Moscow';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const {simulate,resume,Execution,Reconciler,Random,json}=require('../../apps/papertrade-shadow/engine.cjs');
const {createBridge,ROOT,SOURCE_ROOT,sourceManifest,sha}=require('../../apps/papertrade-shadow/strategy.cjs');
const {forensic}=require('../../apps/papertrade-shadow/forensic.cjs');
const {raw,decimal,price,gross,settle}=require('../../apps/papertrade-shadow/money.cjs');
const {safeRead,options,checkLockRecords}=require('../../apps/papertrade-shadow/cli.cjs');
const {START,event,tape}=require('./fixtures.cjs');
const launch=require('../../apps/papertrade-shadow/baseline.json'),config=require('../../apps/papertrade-shadow/scenarios.json');
const experiment=config.experiments[0];
const scenario={...config.scenarios[1],entryMs:[4000],exitMs:[9000],cancelMs:[500],congestion:0,volatility:0,cancelConflictProbability:1};
function engine(s=scenario,e=experiment){const x=new Execution(config,e,s,START);x.bridge=createBridge(x,launch,e,START);return x;}
function load(p){return JSON.parse(fs.readFileSync(path.join(ROOT,p),'utf8'));}

test('fixed precision, negative truncation, raw price scales, no floating realised ledger',()=>{
  assert.equal(decimal(raw('-432.663122459115910303')),'-432.663122459115910303');
  assert.equal(price('81541.5'),815415n);assert.equal(price('2400.12',100n),240012n);
  assert.throws(()=>price('81541.51'));assert.throws(()=>raw('1e-9'));
  const g=gross(raw('2100'),600,815415n,815695n,false);
  assert.ok(Math.abs(Number(decimal(g))+432.6631224591159)<1e-10);
});
test('fees/funding/positive payout are explicit sensitivity hypotheses; liquidation differs from linear',()=>{
  const p={margin:raw('10'),leverage:1000,entry:815745n,exit:815235n,long:true};
  const a=settle(p,scenario),b=settle({...p,liquidated:true},scenario);
  assert.ok(a.cash>b.cash);assert.equal(b.cash,raw('-10'));
  const win=settle({...p,exit:816745n},{positivePayoutBps:5000,winFeeBps:200,fundingBps:5});
  assert.equal(win.cash,win.adjusted-win.fee-win.funding);assert.ok(win.adjusted<win.gross);
});
test('offline original strategy VM blocks signer, unknown modules, network, timers and file writes',()=>{
  const e=engine();try{
    for(const id of ['node:https','node:net','node:child_process','ethers','../runtime.cjs','./papertrade-real'])assert.throws(()=>e.bridge.api.probeRequire(id),/DENIED/);
    assert.throws(()=>e.bridge.api.probeFetch(),/DENIED/);assert.throws(()=>e.bridge.api.probeSigner(),/DENIED/);
    assert.throws(()=>e.bridge.api.probeRequire('fs').writeFileSync('/tmp/key','x'),/DENIED/);
    for(const method of ['binance','hyperliquid','snapshot'])assert.throws(()=>e.bridge.api.probeRequire('../market-data.cjs').marketData[method](),/DENIED/);
  }finally{e.bridge.close();}
});
test('CLI rejects production/private inputs and live/write switches',()=>{
  assert.throws(()=>safeRead('apps/vchera2/.runtime/vchera2.sqlite'));
  assert.throws(()=>options(['--live','true']));assert.throws(()=>options(['--write','true']));
});
test('per-run source lock requires complete current version and rejects stale/duplicate dependencies',()=>{
  const records=[{path:'apps/vchera2/launch-config.json',sha256:sha(fs.readFileSync(path.join(SOURCE_ROOT,'apps/vchera2/launch-config.json')))},...sourceManifest()];
  assert.doesNotThrow(()=>checkLockRecords(records));
  assert.throws(()=>checkLockRecords(records.slice(1)),/cover every/);
  const stale=structuredClone(records);stale[1].sha256='0'.repeat(64);assert.throws(()=>checkLockRecords(stale),/Locked strategy changed/);
  const duplicate=structuredClone(records);duplicate[1]=duplicate[0];assert.throws(()=>checkLockRecords(duplicate),/duplicate/);
});
test('original source signals deterministic, no execution network, identical ledgers and seeded state',async()=>{
  const a=await simulate(tape(),config,launch,experiment,scenario),b=await simulate(tape(),config,launch,experiment,scenario);
  assert.equal(a.summary.ledger_sha256,b.summary.ledger_sha256);assert.deepEqual(a.summary,b.summary);
  assert.ok(a.summary.counts.submitted>0);assert.equal(a.summary.network_requests,0);
  const x=new Random(12),y=new Random(12);for(let i=0;i<30;i++)assert.equal(x.next(),y.next());
});
test('signal gone -> cancel conflict -> late fill -> protective delayed close, exposure persists',async()=>{
  const result=await simulate(tape(),config,launch,experiment,scenario);
  for(const kind of ['cancel_requested','cancel_conflict','entry_fill','close_requested','close_fill'])assert.ok(result.ledger.some(x=>x.kind===kind),kind);
  assert.ok(result.summary.counts.lateFill>0);
  assert.ok(result.positions.every(p=>p.closedAt-p.openedAt>=9000));
  assert.ok(result.positions.some(p=>p.reason.startsWith('запоздалый вход')));
  assert.ok(result.summary.exposure_ms>=9000);
});
test('restart checkpoint by deterministic prefix replay equals uninterrupted execution',async()=>{
  const events=tape(),through=START+182000;
  const prefix=await simulate(events.filter(e=>e.recv_ms<=through),config,launch,experiment,scenario,{until:through});
  const restarted=await resume(prefix.checkpoint,events.filter(e=>e.recv_ms>through));
  const complete=await simulate(events,config,launch,experiment,scenario);
  assert.equal(restarted.summary.ledger_sha256,complete.summary.ledger_sha256);assert.deepEqual(restarted.positions,complete.positions);
  prefix.checkpoint.ledger_sha256='corrupt';await assert.rejects(()=>resume(prefix.checkpoint),/diverged/);
});
test('unknown cancel acceptance retains reserved collateral and pending slot',async()=>{
  const r=await simulate(tape(),config,launch,experiment,{...scenario,cancelConflictProbability:0,cancelAckOnly:true,censored:true});
  assert.ok(r.ledger.some(x=>x.kind==='cancel_accepted_unconfirmed'));assert.equal(r.summary.counts.cancelled,0);
  assert.ok(Number(r.summary.reserved_scenario)>0);assert.ok(r.summary.unresolved_intents.length>0);
  assert.equal(r.summary.counts.submitted,1);
});
test('cancel terminal releases reserve once, queued late timer cannot fill it',async()=>{
  const r=await simulate(tape(),config,launch,experiment,{...scenario,cancelConflictProbability:0});
  assert.ok(r.summary.counts.cancelled>0);assert.equal(r.summary.counts.filled,0);assert.equal(r.summary.reserved_scenario,'0.000000000000000000');
});
test('lost entry/close ack never duplicates identity or double-counts PnL',async()=>{
  const r=await simulate(tape(),config,launch,experiment,{...scenario,lostAck:true});
  assert.ok(r.ledger.some(x=>x.kind==='ack_unknown'));assert.ok(r.ledger.some(x=>x.kind==='close_ack_unknown'));
  assert.equal(new Set(r.positions.map(p=>p.positionId)).size,r.positions.length);
  const e=engine();try{
    const p={positionId:'p',closing:{why:'test'}};
    assert.equal(e.close(p),e.close(p));assert.equal([...e.intents.values()].filter(x=>x.kind==='exit').length,1);
  }finally{e.bridge.close();}
});
test('close terminal, duplicate/out-of-order events and snapshot absence do not resurrect exposure',()=>{
  const r=new Reconciler();r.apply({id:'close',kind:'close',positionId:'1',evidence:'history'});
  r.apply({id:'old-open',kind:'open',positionId:'1',intentId:'i'});
  r.apply({id:'snap',kind:'snapshot',positions:[{positionId:'1',intentId:'i'},{positionId:'2',intentId:'j'}]});
  r.apply({id:'empty',kind:'snapshot',positions:[]});assert.equal(r.positions.size,1);assert.ok(r.positions.has('2'));
  r.apply({id:'close',kind:'close',positionId:'1',evidence:'history'});assert.equal(r.closed.size,1);
  r.apply({id:'partial',kind:'partial',positionId:'2',fraction:0.5});assert.ok(r.positions.has('2'));assert.equal(r.ambiguities.length,1);
  r.apply({id:'ambiguous',kind:'open',positionId:'3'});assert.equal(r.positions.size,1);
});
test('missing depth and stale/gapped feeds block entries rather than fake zero imbalance',async()=>{
  const missing=await simulate(tape({depth:false}),config,launch,experiment,scenario);
  assert.equal(missing.summary.counts.submitted,0);assert.ok(missing.summary.missing_depth_ticks>0);assert.equal(missing.summary.status,'INCONCLUSIVE');
  const e=engine();try{
    const bad=event(START,'hl',{bid:'81800',ask:'81801',bids:[['81800','1']],asks:[['81801','1']]},1,{reconnect:true});
    e.bridge.market(bad);assert.equal(e.bridge.api.feed.hl.bookT,0);
    e.bridge.market({...bad,event_ms:START-10,recv_ms:START+10,quality:{}});assert.ok(e.ledger.some(x=>x.kind==='out_of_order_ignored'));
    e.bridge.market({...bad,event_ms:START+10000});assert.ok(e.ledger.some(x=>x.kind==='clock_skew'));
  }finally{e.bridge.close();}
});
test('unavailable future fill price is censored, never replaced by a future quote',()=>{
  const e=engine();try{
    const q={margin:2100,side:'S'},id=e.submit(q);q.intentId=id;e.bridge.api.pending.push(q);
    e.now=START+4000;e.fire({kind:'entry',id});
    assert.equal(e.intents.get(id).state,'outcome_unknown');assert.equal(e.reserved,raw('2100'));assert.equal(e.positions.size,0);
  }finally{e.bridge.close();}
});
test('end-of-window retains unresolved exposure; no forced mark-to-market realisation',async()=>{
  const r=await simulate(tape({end:184500}),config,launch,experiment,scenario);
  assert.ok(r.summary.open_positions.length>0);assert.ok(r.summary.unresolved_intents.some(x=>x.kind==='exit'));
  assert.equal(r.summary.net_pnl_scenario,'0.000000000000000000');
});
test('insufficient collateral cannot enter full nominal; account money is not position sizing',async()=>{
  const r=await simulate(tape(),config,launch,config.experiments.at(-1),scenario);
  assert.equal(r.summary.counts.filled,0);assert.equal(r.summary.net_pnl_scenario,'0.000000000000000000');
});
test('entry/exit/cancel distributions differ; volume effect explicitly hypothetical and ranks unknown',()=>{
  const s=config.scenarios[2],a=engine(s),b=engine(s);
  try{
    assert.ok(a.delay('entry',1260000)>b.delay('entry',126000));assert.equal(a.summary().measured_counterfactual_rank,null);
    assert.match(s.label,/hypothetical/);assert.notDeepEqual(s.entryMs,s.exitMs);assert.notDeepEqual(s.cancelMs,s.exitMs);
  }finally{a.bridge.close();b.bridge.close();}
});
// Historical account-specific regressions are not distributed with the source-only export.
test('historical absolute cutoff is not moved to tomorrow',async()=>{
  const events=tape().map(x=>({...x,event_ms:x.event_ms+86400000,recv_ms:x.recv_ms+86400000}));
  const r=await simulate(events,config,launch,experiment,scenario);assert.equal(r.summary.counts.submitted,0);
});
test('permuted input has stable causal order; append-only audit deduplicates replayed events',async()=>{
  const events=tape(),a=await simulate(events,config,launch,experiment,scenario),b=await simulate([...events].reverse(),config,launch,experiment,scenario);
  assert.equal(a.summary.ledger_sha256,b.summary.ledger_sha256);
  const c=await simulate([...events,events[0]],config,launch,experiment,scenario);assert.deepEqual(a.positions,c.positions);
});
