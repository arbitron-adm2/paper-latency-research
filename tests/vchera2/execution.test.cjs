'use strict';
const test=require('node:test');
const assert=require('node:assert/strict');
const { WalletView,buildLiveFill,trailingHit,marginCapacity }=require('../../apps/vchera2/execution-state.cjs');
const { ExecutionJournal }=require('../../apps/vchera2/execution-journal.cjs');
const { createCloseSender }=require('../../apps/vchera2/close-request.cjs');
const { makeHarness,tuple,open,evidence,closeTrade }=require('./execution-harness.cjs');
const wallet='0x1111111111111111111111111111111111111111';
const snapshot=positions=>({address:wallet,balance:'2300000000000000000000',funds:{availableBalance:'2300000000000000000000'},sessionKeys:[],operations:[],openPositions:positions});

test('wallet applies position/operation deltas, preserves unchanged fields, rejects wrong wallet',()=>{
  const view=new WalletView(wallet);view.apply(snapshot([]),{initial:true});
  view.apply({addedPositions:[tuple()],addedOperations:[['0xop','openPosition']]});
  assert.equal(view.wallet.openPositions.length,1);assert.equal(view.wallet.operations.length,1);
  view.apply({removedOperationIds:['0xop']});assert.equal(view.wallet.operations.length,0);assert.equal(view.wallet.available,2300);
  view.apply({closedPositions:[['42']]});assert.equal(view.wallet.openPositions.length,0);
  assert.throws(()=>view.apply({...snapshot([]),address:'0xwrong'},{initial:true}),/Wrong wallet/);
  assert.throws(()=>view.apply({balance:'1'},{initial:true}),/Incomplete/);
});
test('terminal close survives stale snapshot and addedPositions repeats',()=>{
  const view=new WalletView(wallet);view.apply(snapshot([tuple()]),{initial:true});
  view.confirmClosed(42,evidence());
  view.apply(snapshot([tuple()]),{initial:true});view.apply({addedPositions:[tuple()]});
  assert.equal(view.wallet.openPositions.length,0);assert.equal(view.closed.size,1);
});
test('real strategy: confirmed close then stale snapshot does not re-adopt or overwrite original reason',async()=>{
  const h=makeHarness(),p=open(h);h.view.apply(snapshot([tuple()]),{initial:true});
  p.closing={why:'Binance развернулся',decidedAt:Date.now()-2000,decidedPx:81570.5};
  h.test.closePx.set('42',evidence());await h.test.liveTick();
  h.view.apply(snapshot([tuple()]),{initial:true});await h.test.liveTick();
  assert.equal(h.test.positions.size,0);assert.equal(h.test.openPosition('book','S',{books:[],fill:{...tuple(),positionId:'42'}}),null);
  const rows=h.db.prepare('SELECT * FROM btcy2_lag_trades').all();
  assert.equal(rows.length,1);assert.equal(rows[0].reason,'Binance развернулся');assert.equal(rows[0].status,'closed');
  assert.equal(JSON.parse(rows[0].exit_info).why,'Binance развернулся');h.db.close();
});
test('wallet absence alone never closes a position or frees its slot; REST repairs missed close event',async()=>{
  const h=makeHarness(),p=open(h);p.opened_at=Date.now()-120000;
  h.view.apply(snapshot([]),{initial:true});await h.test.liveTick();
  assert.equal(h.test.positions.size,1);assert.equal(h.db.prepare('SELECT status FROM btcy2_lag_trades').get().status,'open');
  p.closing={why:'стоп',decidedAt:Date.now()-2000,decidedPx:81586.5};
  h.setHistory([closeTrade()]);await h.test.livePnlSync();await h.test.liveTick();
  const row=h.db.prepare('SELECT * FROM btcy2_lag_trades').get();
  assert.equal(row.status,'closed');assert.equal(row.reason,'стоп');assert.equal(row.closed_at,JSON.parse(row.close_evidence).ts);
  await h.test.livePnlSync();assert.ok(h.db.prepare('SELECT real_pnl FROM btcy2_lag_trades').get().real_pnl < -46);h.db.close();
});
test('close reason and same accepted intent survive process restart without resending',async()=>{
  const h=makeHarness(),p=open(h);p.closing={why:'аварийная остановка',decidedAt:Date.now()-1000,decidedPx:81586.5};
  await h.test.liveTick();assert.equal(h.calls.length,1);
  const row=h.db.prepare('SELECT * FROM btcy2_lag_trades').get();assert.equal(JSON.parse(row.close_state).why,'аварийная остановка');
  const restarted=makeHarness(h.db);restarted.test.orphans.set('0xown',row);restarted.test.adoptOrphans();await restarted.test.liveTick();
  assert.equal(restarted.calls.length,0);assert.equal(restarted.test.positions.size,1);
  restarted.test.closePx.set('42',evidence());await restarted.test.liveTick();
  assert.equal(h.db.prepare('SELECT reason FROM btcy2_lag_trades').get().reason,'аварийная остановка');h.db.close();
});
test('kill closes a known own position even if wallet snapshot has not seen its open',async()=>{
  const h=makeHarness();open(h);assert.equal(h.account.wallet.openPositions.length,0);
  const result=await h.api.killSwitch();assert.equal(result.closing,1);assert.equal(h.calls.length,1);
  assert.equal(h.test.positions.size,1);assert.equal(h.test.cfg.live,false);
  const row=h.db.prepare('SELECT close_state FROM btcy2_lag_trades').get();assert.equal(JSON.parse(row.close_state).why,'аварийная остановка');h.db.close();
});
test('signal gone -> cancel conflict -> fill: record causality, close immediately and hold slot until fill',async()=>{
  const h=makeHarness();
  const q={live:true,strategy:'book',side:'S',intentId:'0xown',margin:2100,px:81583.5,createdAt:Date.now()-4000,signalGone:true,cancelFailed:'intent_conflict',cancelTries:1,cancelRequestedAt:Date.now()-1000,ctx:{books:[],dev:1.2,binMove:0}};
  h.test.pending.push(q);h.view.apply(snapshot([tuple()]),{initial:true});
  await h.test.liveTick();await new Promise(resolve=>setImmediate(resolve));
  assert.equal(h.test.pending.length,0);assert.equal(h.calls.length,1);assert.equal(h.test.positions.size,1);
  let row=h.db.prepare('SELECT * FROM btcy2_lag_trades').get();
  const entry=JSON.parse(row.entry_info),closing=JSON.parse(row.close_state);
  assert.equal(entry.signalGone,true);assert.equal(entry.cancelFailed,'intent_conflict');assert.equal(entry.cancelTries,1);
  assert.match(closing.why,/запоздалый вход/);assert.match(closing.why,/intent_conflict/);assert.equal(row.status,'open');
  h.test.closePx.set('42',evidence());await h.test.liveTick();
  row=h.db.prepare('SELECT * FROM btcy2_lag_trades').get();assert.equal(row.status,'closed');assert.match(row.reason,/запоздалый вход/);h.db.close();
});
test('normal fill does not force-close; invalidation metadata is explicit',()=>{
  const q={intentId:'0xown',createdAt:1,px:100};assert.equal(buildLiveFill(q,{}).invalidatedReason,null);
  assert.match(buildLiveFill({...q,signalGone:true,cancelFailed:'intent_conflict'},{}).invalidatedReason,/сигнал/);
});
test('close lost acknowledgement uses same durable message/nonce/intent and persists before network',async()=>{
  const sent=[],writes=[];let nonce=0;
  const close=createCloseSender({master:wallet,takeNonce:()=>String(++nonce),deadline:()=>'123',sign:async msg=>({signed:msg,intentId:'0xsame'}),post:async body=>{assert.equal(writes.length,sent.length+1);sent.push(body);return sent.length===1?{ok:false,uncertain:true,data:{}}:{ok:true,data:{intentId:'0xsame'}};}});
  let state;
  const save=x=>{state=structuredClone(x);writes.push(state);};
  await close(['42'],{onPrepared:save});await close(['42'],{request:state.message,onPrepared:save});
  assert.equal(nonce,1);assert.deepEqual(sent[0],sent[1]);assert.equal(writes[0].intentId,writes[1].intentId);
  await assert.rejects(close(['99'],{request:state.message}),/identity mismatch/);
});
test('failed durable write prevents HTTP submission',async()=>{
  let sent=0;const close=createCloseSender({master:wallet,takeNonce:()=>'1',deadline:()=>'123',sign:async()=>({signed:{},intentId:'0xid'}),post:async()=>{sent++;return{ok:true,data:{}};}});
  await assert.rejects(close(['42'],{onPrepared:()=>{throw new Error('disk full');}}),/disk full/);assert.equal(sent,0);
});
test('duplicate repair retains first row, restores reason and original timing, archives originals and enforces uniqueness',()=>{
  const h=makeHarness();const p=open(h);h.db.exec('DROP INDEX btcy2_live_position_once');
  h.db.prepare("UPDATE btcy2_lag_trades SET status='reopened',exit_info=? WHERE id=?").run(JSON.stringify({decidedAt:100,decidedPx:81580,sentAt:200}),p.id);
  h.db.exec("INSERT INTO btcy2_lag_trades(opened_at,side,entry,qty,margin,leverage,live,position_id,intent_id,status,reason) VALUES(1,'S',81583.5,15,2100,600,1,'42','0xown','closed','время')");
  const journal=new ExecutionJournal(h.db);journal.repairDuplicates(new Map([['42',{px:81586.5,ts:300,realPnl:-46.332898,source:'account-history'}]]),{'42':'аварийная остановка'});
  const canonical=journal.find('42');assert.equal(canonical.id,p.id);assert.equal(canonical.reason,'аварийная остановка');
  assert.equal(JSON.parse(canonical.exit_info).decidedAt,100);assert.equal(canonical.closed_at,300);
  assert.equal(h.db.prepare("SELECT COUNT(*) n FROM btcy2_lag_trades WHERE status='duplicate'").get().n,1);
  assert.equal(h.db.prepare('SELECT COUNT(*) n FROM btcy2_execution_audit').get().n,2);
  assert.throws(()=>h.db.exec("INSERT INTO btcy2_lag_trades(opened_at,side,entry,qty,margin,leverage,live,position_id,status) VALUES(1,'S',1,1,1,1,1,'42','open')"),/UNIQUE/);
  assert.deepEqual(journal.repairDuplicates(new Map()),[]);h.db.close();
});
test('full margin includes unchanged 5% reserve; trailer at 6.5 works with zero start threshold',()=>{
  assert.equal(marginCapacity(2172.51606).ready,false);assert.equal(marginCapacity(2218.848958).ready,true);
  assert.ok(marginCapacity(2172.51606).shortfall>38);
  assert.equal(trailingHit({trailUsd:6.5,trailStartUsd:0},7,0.5),true);
  assert.equal(trailingHit({trailUsd:6.5,trailStartUsd:0},0,-10),false);
  assert.equal(trailingHit({trailUsd:6.5,trailStartUsd:20},7,0),false);
});
