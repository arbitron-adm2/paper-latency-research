'use strict';
const fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto');
const {DatabaseSync}=require('node:sqlite');
const C=require('./state-codec.cjs');
const {allowedRoots}=require('./telemetry.cjs');
const DB_NAME='shadow-history.sqlite';
function allowedDirectory(dir){
  const p=path.resolve(dir),parent=fs.realpathSync(fs.existsSync(p)?p:path.dirname(p));
  if(p.includes('/.runtime/')||!allowedRoots().some(r=>parent===r||parent.startsWith(r+path.sep)))throw new Error('STATE_DIRECTORY_DENIED');
  if(fs.existsSync(p)&&fs.lstatSync(p).isSymbolicLink())throw new Error('STATE_SYMLINK_DENIED');return p;
}
function ownFile(dir,name){const p=path.join(dir,name);if(fs.existsSync(p)&&(!fs.lstatSync(p).isFile()||fs.lstatSync(p).isSymbolicLink()))throw new Error('STATE_FILE_DENIED');return p;}
function alive(pid){try{process.kill(pid,0);return true;}catch(e){return e.code!=='ESRCH';}}
class History{
  constructor(dir,{now=Date.now(),maxDbBytes=4294967296,isAlive=alive}={}){
    this.dir=allowedDirectory(dir);this.maxDbBytes=maxDbBytes;this.closed=false;this.inTransaction=false;
    const created=!fs.existsSync(this.dir);if(created)fs.mkdirSync(this.dir,{mode:0o700});
    const marker=ownFile(this.dir,'state.json');
    if(created)fs.writeFileSync(marker,JSON.stringify({schemaVersion:1,type:'PERSISTENT_SHADOW_SIM',accountId:crypto.randomUUID(),createdMs:now,mode:'READ_ONLY_RESEARCH_NO_ORDERS'})+'\n',{flag:'wx',mode:0o600});
    if(!fs.existsSync(marker))throw new Error('EXISTING_DIRECTORY_HAS_NO_SHADOW_OWNERSHIP');
    this.marker=JSON.parse(fs.readFileSync(marker,'utf8'));
    if(this.marker.schemaVersion!==1||this.marker.type!=='PERSISTENT_SHADOW_SIM'||this.marker.mode!=='READ_ONLY_RESEARCH_NO_ORDERS')throw new Error('STATE_MARKER_MISMATCH');
    this.file=ownFile(this.dir,DB_NAME);if(!created&&!fs.existsSync(this.file))throw new Error('HISTORY_MISSING_NO_FREE_RESET');
    this.lockFile=path.join(this.dir,'owner.lock');this.token=crypto.randomUUID();
    if(fs.existsSync(this.lockFile)){
      if(fs.lstatSync(this.lockFile).isSymbolicLink())throw new Error('OWNER_LOCK_SYMLINK');
      const old=JSON.parse(fs.readFileSync(this.lockFile,'utf8'));
      if(!Number.isSafeInteger(old.pid)||old.pid<1||isAlive(old.pid))throw new Error('SHADOW_SINGLE_OWNER_LOCKED');
      this.staleOwner=old;fs.unlinkSync(this.lockFile);
    }
    fs.writeFileSync(this.lockFile,JSON.stringify({pid:process.pid,token:this.token,atMs:now})+'\n',{flag:'wx',mode:0o600});
    try{
      ownFile(this.dir,DB_NAME+'-wal');ownFile(this.dir,DB_NAME+'-shm');
      this.db=new DatabaseSync(this.file);this.db.exec('PRAGMA busy_timeout=1000; PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA wal_autocheckpoint=1000;');
      this.db.exec(fs.readFileSync(path.join(__dirname,'history-schema.sql'),'utf8'));
      const account=this.getMeta('account_id');if(account&&account!==this.marker.accountId)throw new Error('STATE_ACCOUNT_MISMATCH');
      if(!account)this.setMeta('account_id',this.marker.accountId);
      this.runId=this.getMeta('active_run')??this.marker.accountId;
      this.newAccount=!this.getMeta('active_run');
      if(this.newAccount)this.setMeta('active_run',this.runId);
      this.lastSafety=this.readSafety();
    }catch(e){this.db?.close();this.release();throw e;}
  }
  getMeta(k){return this.db.prepare('SELECT value FROM sim_meta WHERE key=?').get(k)?.value??null;}
  setMeta(k,v){this.db.prepare('INSERT INTO sim_meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value').run(k,String(v));}
  checkSpace(){let bytes=fs.statSync(this.file).size;const wal=this.file+'-wal';if(fs.existsSync(wal))bytes+=fs.statSync(wal).size;if(bytes>this.maxDbBytes)throw new Error('HISTORY_DB_QUOTA');}
  transaction(fn){
    if(this.closed)throw new Error('HISTORY_CLOSED');this.checkSpace();
    if(this.inTransaction)return fn();
    const previousSafety=this.lastSafety;this.db.exec('BEGIN IMMEDIATE');this.inTransaction=true;
    try{const result=fn();this.db.exec('COMMIT');this.inTransaction=false;return result;}
    catch(e){try{this.db.exec('ROLLBACK');}catch{}this.inTransaction=false;this.lastSafety=previousSafety;throw e;}
  }
  begin(profile,now){
    const previous=this.db.prepare('SELECT * FROM sim_runs WHERE run_id=?').get(this.runId);
    this.transaction(()=>{
      this.db.prepare('INSERT OR IGNORE INTO sim_models(hash,created_ms,profile_json) VALUES(?,?,?)').run(profile.modelHash,now,C.stringify(profile));
      this.db.prepare('INSERT OR IGNORE INTO sim_configs(hash,created_ms,config_json) VALUES(?,?,?)').run(profile.configHash,now,C.stringify(profile.config));
      if(!previous)this.db.prepare("INSERT INTO sim_runs(run_id,created_ms,last_ms,status,model_hash,config_hash) VALUES(?,?,?,'STARTING',?,?)").run(this.runId,now,now,profile.modelHash,profile.configHash);
      else this.db.prepare("UPDATE sim_runs SET restarts=restarts+1,last_ms=?,status='RESUMING' WHERE run_id=?").run(now,this.runId);
      this.segment=Number(this.db.prepare("INSERT INTO sim_segments(run_id,started_ms,status,model_hash,config_hash) VALUES(?,?,'STARTING',?,?)").run(this.runId,now,profile.modelHash,profile.configHash).lastInsertRowid);
    });
    this.profile=profile;return {previous,newAccount:this.newAccount};
  }
  installNativeEpoch(){
    for(const {name} of this.db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'btcy2_%'").all()){
      if(!/^btcy2_[a-z0-9_]+$/.test(name))throw new Error('NATIVE_TABLE_NOT_REVIEWED');
      for(const op of ['INSERT','UPDATE','DELETE'])this.db.exec('CREATE TRIGGER IF NOT EXISTS "sim_epoch_'+name+'_'+op+'" AFTER '+op+' ON "'+name+'" BEGIN INSERT INTO sim_meta(key,value) VALUES(\'native_epoch\',\'1\') ON CONFLICT(key) DO UPDATE SET value=CAST(value AS INTEGER)+1; END');
    }
  }
  quality(kind,payload,now){this.db.prepare('INSERT INTO quality_events(run_id,at_ms,kind,payload_json) VALUES(?,?,?,?)').run(this.runId,now,kind,C.stringify(payload));}
  appendNote(note,safety){
    const payload=C.stringify(note),state=C.stringify(safety);
    this.transaction(()=>{
      this.db.prepare('INSERT OR IGNORE INTO sim_events(run_id,event_sequence,at_ms,kind,payload_json) VALUES(?,?,?,?,?)').run(this.runId,note.sequence,note.time_ms,note.kind,payload);
      this.db.prepare('INSERT INTO sim_safety(run_id,at_ms,event_sequence,sha256,payload_json) VALUES(?,?,?,?,?) ON CONFLICT(run_id) DO UPDATE SET at_ms=excluded.at_ms,event_sequence=excluded.event_sequence,sha256=excluded.sha256,payload_json=excluded.payload_json').run(this.runId,note.time_ms,note.sequence,C.hash(state),state);
      this.syncRisk(safety);
    });this.lastSafety=C.clone(safety);
  }
  syncRisk(s){
    for(const [id,r] of s.intents){this.db.prepare('INSERT INTO sim_intents(run_id,intent_id,kind,state,created_ms,notional_raw,deadline_sec,payload_json) VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(run_id,intent_id) DO UPDATE SET state=excluded.state,payload_json=excluded.payload_json').run(this.runId,id,r.kind,r.state,r.createdAt,r.notional==null?null:String(r.notional),r.entryLimit?.deadlineSec??null,C.stringify(r));}
    for(const [id,p] of s.positions){this.db.prepare('INSERT INTO sim_positions(run_id,position_id,status,opened_ms,margin_raw,payload_json) VALUES(?,?,?,?,?,?) ON CONFLICT(run_id,position_id) DO UPDATE SET status=excluded.status,payload_json=excluded.payload_json').run(this.runId,id,p.censored?'censored':'open',p.openedAt,String(p.margin),C.stringify(p));}
    for(const p of s.orders??[]){
      this.db.prepare('INSERT OR IGNORE INTO sim_trades(run_id,position_id,opened_ms,closed_ms,entry_raw,exit_raw,margin_raw,leverage,cash_raw,fee_raw,reason,model_hash) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)').run(this.runId,p.positionId,p.openedAt,p.closedAt,p.entry_raw,p.exit_raw,p.margin_raw,p.leverage,p.cash,p.fee,p.reason,this.profile.modelHash);
      this.db.prepare("UPDATE sim_positions SET status='closed',closed_ms=?,payload_json=? WHERE run_id=? AND position_id=?").run(p.closedAt,C.stringify(p),this.runId,p.positionId);
    }
  }
  readSafety(){
    const r=this.db.prepare('SELECT * FROM sim_safety WHERE run_id=?').get(this.runId);if(!r)return null;
    if(C.hash(r.payload_json)!==r.sha256)throw new Error('SAFETY_STATE_CHECKSUM');return C.parse(r.payload_json);
  }
  checkpoint(value,{now,clean=false}={}){
    const blob=C.stringify(value),generation=Number(this.getMeta('checkpoint_generation')??0)+1;
    this.transaction(()=>{
      this.db.prepare('INSERT INTO sim_checkpoints(run_id,slot,generation,saved_ms,clean,model_hash,sha256,payload_json) VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(run_id,slot) DO UPDATE SET generation=excluded.generation,saved_ms=excluded.saved_ms,clean=excluded.clean,model_hash=excluded.model_hash,sha256=excluded.sha256,payload_json=excluded.payload_json').run(this.runId,generation%2,generation,now,clean?1:0,this.profile.modelHash,C.hash(blob),blob);
      this.setMeta('checkpoint_generation',generation);this.syncRisk(value.session.engine);
      this.db.prepare('UPDATE sim_runs SET last_ms=?,status=? WHERE run_id=?').run(now,value.observerStatus??'RUNNING',this.runId);
    });return {generation,savedAtMs:now,clean,sha256:C.hash(blob)};
  }
  recovery(){
    const rows=this.db.prepare('SELECT * FROM sim_checkpoints WHERE run_id=? ORDER BY generation DESC').all(this.runId),corrupt=[];
    for(const r of rows){try{if(C.hash(r.payload_json)!==r.sha256)throw new Error('CHECKSUM');return {state:C.parse(r.payload_json),savedAtMs:r.saved_ms,clean:!!r.clean,modelHash:r.model_hash,generation:r.generation,corrupt,safety:this.readSafety()};}catch(e){corrupt.push({generation:r.generation,error:e.message});}}
    return {state:null,safety:this.readSafety(),clean:false,corrupt,reason:rows.length?'ALL_CHECKPOINTS_CORRUPT':'NO_CHECKPOINT'};
  }
  decision(c,now){
    const second=Math.floor(now/1000)*1000,why=c.baselineReason??'NOT_EVALUATED',tr=(c.timingReasons??[]).join('|'),er=c.entryReason??'';
    this.db.prepare('INSERT INTO sim_decisions(run_id,second_ms,strategy,side,raw_trigger,baseline_ok,baseline_reason,timing_status,timing_reason,entry_reason,signal_min,signal_max,available_raw,notional_usd,model_hash) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(run_id,second_ms,strategy,side,baseline_reason,timing_status,timing_reason,entry_reason) DO UPDATE SET observations=observations+1,signal_min=MIN(signal_min,excluded.signal_min),signal_max=MAX(signal_max,excluded.signal_max),available_raw=excluded.available_raw').run(this.runId,second,c.strategy,c.side,1,c.baselineOk==null?null:(c.baselineOk?1:0),why,c.timingStatus??'SUPPRESSED',tr,er,Number.isFinite(c.signal)?c.signal:null,Number.isFinite(c.signal)?c.signal:null,String(c.availableRaw),1260000,this.profile.modelHash);
  }
  market(s){
    this.db.prepare('INSERT INTO market_seconds(run_id,second_ms,symbol,venue,quote_n,book_n,bid_min,bid_max,ask_min,ask_max,last_bid,last_ask,last_recv_ms) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(run_id,second_ms,symbol,venue) DO UPDATE SET quote_n=quote_n+excluded.quote_n,book_n=book_n+excluded.book_n,bid_min=MIN(bid_min,excluded.bid_min),bid_max=MAX(bid_max,excluded.bid_max),ask_min=MIN(ask_min,excluded.ask_min),ask_max=MAX(ask_max,excluded.ask_max),last_bid=excluded.last_bid,last_ask=excluded.last_ask,last_recv_ms=excluded.last_recv_ms').run(this.runId,s.second,s.symbol,s.venue,s.quoteN,s.bookN,s.bidMin,s.bidMax,s.askMin,s.askMax,s.lastBid,s.lastAsk,s.recvMs);
  }
  evidence(e,boot){
    const p=e.payload,cohort={kind:p.kind??p.event??null,instrumentId:p.instrumentId??null,positionId:p.positionId??p.args?.positionId??null,
      attribution:'PUBLIC_NOT_OWN_VIRTUAL_FILL',personalQueue:'UNKNOWN',volumePriority:'INCONCLUSIVE'};
    this.db.prepare('INSERT OR IGNORE INTO public_evidence(run_id,boot,sequence,kind,symbol,recv_ms,source_ms,payload_hash,cohort_json) VALUES(?,?,?,?,?,?,?,?,?)').run(this.runId,boot,e.sequence,e.kind,e.symbol,e.recv_ms,e.source_ms,e.payload_sha256,C.stringify(cohort));
  }
  forecast(p){
    const minimal={...p};delete minimal.input_pressure;
    this.db.prepare('INSERT OR IGNORE INTO sim_forecasts(run_id,boot,sequence,at_ms,model_hash,input_hash,payload_json) VALUES(?,?,?,?,?,?,?)').run(this.runId,p.bootId,p.sequence,p.prediction_at_ms,p.model_config_sha256,p.input_sha256,C.stringify(minimal));
  }
  hasRisk(){
    const own=!!this.db.prepare("SELECT (SELECT COUNT(*) FROM sim_intents WHERE run_id=? AND state IN('submitted','queued','outcome_unknown'))+(SELECT COUNT(*) FROM sim_positions WHERE run_id=? AND status IN('open','censored')) n").get(this.runId,this.runId).n;
    if(own)return true;
    const has=name=>!!this.db.prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?").get(name);
    if(!has('sim_strategy_accounts'))return false;
    // Archived comparison models are frozen evidence, not live risk: they must not pin raw context (quota) forever.
    const sql=has('sim_strategy_archived')?'SELECT 1 FROM sim_strategy_accounts a WHERE a.unresolved>0 AND NOT EXISTS(SELECT 1 FROM sim_strategy_archived x WHERE x.model_id=a.model_id) LIMIT 1':'SELECT 1 FROM sim_strategy_accounts WHERE unresolved>0 LIMIT 1';
    return !!this.db.prepare(sql).get();
  }
  rollup(now){
    const cutoff=Math.floor((now-7*86400000)/86400000)*86400000;
    this.transaction(()=>{
      this.db.prepare('INSERT INTO market_daily(run_id,day_ms,symbol,venue,quote_n,book_n,seconds_n,bid_min,bid_max,ask_min,ask_max) SELECT run_id,CAST(second_ms/86400000 AS INTEGER)*86400000,symbol,venue,SUM(quote_n),SUM(book_n),COUNT(*),MIN(bid_min),MAX(bid_max),MIN(ask_min),MAX(ask_max) FROM market_seconds WHERE second_ms<? GROUP BY run_id,CAST(second_ms/86400000 AS INTEGER),symbol,venue ON CONFLICT(run_id,day_ms,symbol,venue) DO UPDATE SET quote_n=quote_n+excluded.quote_n,book_n=book_n+excluded.book_n,seconds_n=seconds_n+excluded.seconds_n,bid_min=MIN(bid_min,excluded.bid_min),bid_max=MAX(bid_max,excluded.bid_max),ask_min=MIN(ask_min,excluded.ask_min),ask_max=MAX(ask_max,excluded.ask_max)').run(cutoff);
      this.db.prepare('DELETE FROM market_seconds WHERE second_ms<?').run(cutoff);
    });
  }
  totals(){const out={};for(const name of ['sim_decisions','sim_events','sim_intents','sim_positions','sim_trades','sim_forecasts','public_evidence','quality_events','market_seconds','market_daily'])out[name]=this.db.prepare('SELECT COUNT(*) n FROM '+name).get().n;out.candidateObservations=this.db.prepare('SELECT COALESCE(SUM(observations),0) n FROM sim_decisions').get().n;return out;}
  finish(status,now,recovery){this.transaction(()=>{this.db.prepare('UPDATE sim_segments SET stopped_ms=?,status=?,recovery_json=? WHERE id=?').run(now,status,C.stringify(recovery),this.segment);this.db.prepare('UPDATE sim_runs SET last_ms=?,status=? WHERE run_id=?').run(now,status,this.runId);});}
  release(){if(fs.existsSync(this.lockFile)){try{const x=JSON.parse(fs.readFileSync(this.lockFile,'utf8'));if(x.token===this.token)fs.unlinkSync(this.lockFile);}catch{}}}
  close(){if(this.closed)return;this.closed=true;try{this.db?.close();}finally{this.release();}}
}
function queryHistory(dir,{limit=20}={}){
  if(!Number.isInteger(limit)||limit<1||limit>1000)throw new Error('HISTORY_QUERY_LIMIT');
  dir=allowedDirectory(dir);const file=ownFile(dir,DB_NAME);if(!fs.existsSync(file))throw new Error('HISTORY_NOT_FOUND');
  const db=new DatabaseSync(file,{readOnly:true});
  try{db.exec('PRAGMA query_only=ON; PRAGMA busy_timeout=1000;');
    const runs=db.prepare('SELECT run_id,created_ms,last_ms,status,restarts,model_hash,config_hash,financial_mode FROM sim_runs ORDER BY created_ms DESC LIMIT 10').all();
    const segments=db.prepare('SELECT id,run_id,started_ms,stopped_ms,status,model_hash,config_hash FROM sim_segments ORDER BY id DESC LIMIT 10').all();
    const trades=db.prepare('SELECT run_id,position_id,opened_ms,closed_ms,entry_raw,exit_raw,margin_raw,leverage,cash_raw,fee_raw,reason,model_hash,financial_mode FROM sim_trades ORDER BY closed_ms DESC LIMIT ?').all(limit);
    const totals={};for(const name of ['sim_trades','sim_decisions','sim_forecasts','public_evidence','quality_events','market_seconds','market_daily'])totals[name]=db.prepare('SELECT COUNT(*) n FROM '+name).get().n;
    let comparison={models:[],trades:[]};
    if(db.prepare("SELECT 1 FROM sqlite_master WHERE name='sim_strategy_accounts'").get()){
      comparison.models=db.prepare('SELECT model_id,asset,family,timing_policy,model_hash,created_ms,updated_ms,unresolved,financial_mode FROM sim_strategy_accounts ORDER BY model_id').all();
      comparison.trades=db.prepare('SELECT model_id,position_id,asset,opened_ms,closed_ms,cash_raw,financial_mode FROM sim_strategy_trades ORDER BY closed_ms DESC LIMIT ?').all(limit);
      comparison.meaning='Independent SIM experiments; not a funded combined portfolio';
    }
    return {schemaVersion:1,mode:'READ_ONLY_RESEARCH_NO_ORDERS',financialMutations:0,realSignatures:0,financialPerformance:'SIM_NOT_CASH_PAYOUT',runs,segments,totals,trades,comparison,internalPayloadsIncluded:false};
  }finally{db.close();}
}
module.exports={History,queryHistory,allowedDirectory,ownFile,DB_NAME};
