'use strict';
// Synthetic regression for the two live shadow slowdowns found 21.10 (raw prune full scans; stale-note fsync storm). NOT performance evidence.
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const {History}=require('../../apps/papertrade-shadow/history.cjs');
const {buildProfile}=require('../../apps/papertrade-shadow/persistent-profile.cjs');
const {RawRetention}=require('../../apps/papertrade-shadow/raw-retention.cjs');
const {ROOT}=require('../../apps/papertrade-shadow/strategy.cjs');
const {validateEvent}=require('../../apps/papertrade-shadow/live-feed.cjs');
const {IncrementalSession}=require('../../apps/papertrade-shadow/incremental.cjs');
const launch=require('../../apps/papertrade-shadow/baseline.json'),base=require('../../apps/papertrade-shadow/scenarios.json');
const {START}=require('./fixtures.cjs');
const crypto=require('node:crypto');
function tempState(){const parent=fs.mkdtempSync(path.join(ROOT,'reports/perf-unit-'));return {parent,dir:path.join(parent,'state')};}
test('raw prune uses the partial live-row index and keeps deletion semantics (deleted history untouched)',()=>{
  const x=tempState();let h;try{
    h=new History(x.dir,{now:START});h.begin(buildProfile(),START);
    const policy={...buildProfile().rawPolicy,quotaBytes:10*1024*1024},raw=new RawRetention(h,policy,{onWarning:()=>{},onRecovered:()=>{}});
    const ins=h.db.prepare('INSERT INTO raw_segments(run_id,file,sha256,bytes,first_ms,last_ms,event_n,coverage,protected,context_json,deleted_ms) VALUES(?,?,?,?,?,?,?,?,?,?,?)');
    for(let k=0;k<500;k++)ins.run('old','raw/segment-old-'+k+'.jsonl.gz','0',100,START-9e6,START-9e6,1,'X',0,'x'.repeat(1000),START-8e6);
    ins.run('live','raw/segment-live-old.jsonl.gz','0',100,START-9e6,START-9e6,1,'X',1,'{}',null);   // expired, protected, no risk -> released and deleted
    ins.run('live','raw/segment-live-new.jsonl.gz','0',100,START,START,1,'X',0,'{}',null);           // fresh -> kept
    for(const q of ["UPDATE raw_segments SET protected=0 WHERE deleted_ms IS NULL AND protected<>0","UPDATE raw_segments SET protected=0 WHERE deleted_ms IS NULL AND protected=1 AND last_ms<1","SELECT id,file,bytes,last_ms FROM raw_segments WHERE deleted_ms IS NULL AND protected=0 ORDER BY id"]){
      const plan=h.db.prepare('EXPLAIN QUERY PLAN '+q).all().map(r=>r.detail).join(';');
      assert.match(plan,/USING INDEX raw_segments_live/,q);
    }
    const before=h.db.prepare("SELECT sum(deleted_ms) s FROM raw_segments WHERE run_id='old'").get().s;
    raw.prune(START+1000);
    assert.equal(h.db.prepare("SELECT deleted_ms IS NOT NULL d FROM raw_segments WHERE file='raw/segment-live-old.jsonl.gz'").get().d,1);
    assert.equal(h.db.prepare("SELECT deleted_ms IS NULL d FROM raw_segments WHERE file='raw/segment-live-new.jsonl.gz'").get().d,1);
    assert.equal(h.db.prepare("SELECT sum(deleted_ms) s FROM raw_segments WHERE run_id='old'").get().s,before,'already deleted rows are not rewritten');
  }finally{h?.close();fs.rmSync(x.parent,{recursive:true,force:true});}
});
const scenario={...base.scenarios[1],entryMs:[4000],exitMs:[9000],cancelMs:[500],congestion:0,volatility:0,cancelConflictProbability:1};
const config={...base,realtime:{maxReceiveAgeMs:2500},entryDeadline:{signalTtlMs:15000,safetySeconds:1}};
function ev(seq,t){const p={bid:'81799.5',ask:'81800.5',frameKind:'book',instrumentId:0,priceScale:'10',sourceTimestampKnown:true,bids:[['81799.5','1']],asks:[['81800.5','1']]};
  return validateEvent({schemaVersion:1,sequence:seq,kind:'market',symbol:'BTC',venue:'hl',recv_ms:t,source_ms:t,payload:p,payload_sha256:crypto.createHash('sha256').update(JSON.stringify(p)).digest('hex'),provenance:'synthetic'});}
test('stale storm: reset/warmup apply on every stale event, the journal gets one coalesced note until fresh input or 5 s',()=>{
  const s=new IncrementalSession(config,launch,base.experiments[0],scenario,START);try{
    s.setContinuity(true);assert.ok(s.ingest(ev(1,START),START));
    const notes=()=>s.engine.ledger.filter(r=>r.kind==='input_invalidated');
    for(let k=0;k<5;k++)s.ingest(ev(2+k,START+k*10),START+3000+k*10);
    assert.equal(s.inputCounts.rejectedStale,5);
    assert.equal(notes().length,1,'one journal note for the whole storm');
    assert.equal(s.engine.integrityWarmUntil,START+3040+180000,'warmup still extended by the last stale event');
    assert.equal(s.engine.inputHealthy,false);
    assert.ok(s.ingest(ev(10,START+3100),START+3100),'fresh input accepted');
    s.ingest(ev(11,START+3100),START+6000);
    assert.equal(notes().length,2);assert.equal(notes().at(-1).coalescedRepeats,4,'suppressed repeats are counted on the next note');
    s.ingest(ev(12,START+3100),START+12000);
    assert.equal(notes().length,3,'a still-running storm is re-noted after 5 s');
  }finally{s.close();}
});
