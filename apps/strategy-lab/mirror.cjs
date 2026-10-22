'use strict';
// Research projection only: reads the operational SIM journal, never its runtime/auth files.
const fs=require('node:fs'),path=require('node:path'),{DatabaseSync}=require('node:sqlite');
function mirrorShadow(target,dir){
 const file=path.join(dir,'shadow-history.sqlite');if(!fs.existsSync(file))return;const account=JSON.parse(fs.readFileSync(path.join(dir,'state.json'),'utf8')).accountId;if(typeof account!=='string')throw new Error('SIM_SOURCE_ACCOUNT_REQUIRED');
 target.exec(`CREATE TABLE IF NOT EXISTS strategy_shadow_accounts(source_account TEXT,model_id TEXT,asset TEXT,model_hash TEXT,updated_ms REAL,unresolved INTEGER,snapshot_json TEXT,financial_mode TEXT NOT NULL CHECK(financial_mode='SIM'),PRIMARY KEY(source_account,model_id));
 CREATE TABLE IF NOT EXISTS strategy_shadow_events(source_account TEXT,model_id TEXT,sequence INTEGER,at_ms REAL,kind TEXT,payload_json TEXT,financial_mode TEXT NOT NULL CHECK(financial_mode='SIM'),PRIMARY KEY(source_account,model_id,sequence));
 CREATE TABLE IF NOT EXISTS strategy_shadow_trades(source_account TEXT,model_id TEXT,position_id TEXT,asset TEXT,opened_ms REAL,closed_ms REAL,cash_raw TEXT,payload_json TEXT,financial_mode TEXT NOT NULL CHECK(financial_mode='SIM'),PRIMARY KEY(source_account,model_id,position_id));
 CREATE TABLE IF NOT EXISTS strategy_shadow_definitions(source_account TEXT,model_id TEXT,created_ms REAL,definition_json TEXT,PRIMARY KEY(source_account,model_id));`);
 const src=new DatabaseSync(file,{readOnly:true});src.exec('PRAGMA query_only=ON;PRAGMA busy_timeout=0');
 try{
  src.exec('BEGIN');const accounts=src.prepare('SELECT model_id,asset,model_hash,updated_ms,unresolved,snapshot_json,financial_mode FROM sim_strategy_accounts').all(),pending=[];
  for(const row of accounts){if(row.financial_mode!=='SIM')throw new Error('NON_SIM_SOURCE_REFUSED');const after=target.prepare('SELECT MAX(sequence) n FROM strategy_shadow_events WHERE source_account=? AND model_id=?').get(account,row.model_id).n??0;const events=src.prepare('SELECT sequence,at_ms,kind,payload_json,financial_mode FROM sim_strategy_events WHERE model_id=? AND sequence>? ORDER BY sequence LIMIT 2000').all(row.model_id,after);const trades=src.prepare('SELECT * FROM sim_strategy_trades WHERE model_id=?').all(row.model_id);pending.push({row,events,trades});}
  const defs=src.prepare('SELECT model_id,created_ms,definition_json FROM sim_strategy_definitions').all();target.exec('BEGIN IMMEDIATE');try{
   for(const {row:r,events,trades}of pending){target.prepare('INSERT INTO strategy_shadow_accounts VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(source_account,model_id) DO UPDATE SET model_hash=excluded.model_hash,updated_ms=excluded.updated_ms,unresolved=excluded.unresolved,snapshot_json=excluded.snapshot_json WHERE updated_ms<>excluded.updated_ms').run(account,r.model_id,r.asset,r.model_hash,r.updated_ms,r.unresolved,r.snapshot_json,'SIM');
    for(const e of events){if(e.financial_mode!=='SIM')throw new Error('NON_SIM_EVENT_REFUSED');target.prepare('INSERT OR IGNORE INTO strategy_shadow_events VALUES(?,?,?,?,?,?,?)').run(account,r.model_id,e.sequence,e.at_ms,e.kind,e.payload_json,'SIM');}
    for(const t of trades){if(t.financial_mode!=='SIM')throw new Error('NON_SIM_TRADE_REFUSED');target.prepare('INSERT OR IGNORE INTO strategy_shadow_trades VALUES(?,?,?,?,?,?,?,?,?)').run(account,r.model_id,t.position_id,t.asset,t.opened_ms,t.closed_ms,t.cash_raw,t.payload_json,'SIM');}}
   for(const d of defs)target.prepare('INSERT OR IGNORE INTO strategy_shadow_definitions VALUES(?,?,?,?)').run(account,d.model_id,d.created_ms,d.definition_json);
   target.exec('COMMIT');return{account,models:accounts.length,events:pending.reduce((n,x)=>n+x.events.length,0)};
  }catch(e){target.exec('ROLLBACK');throw e;}
 }finally{if(src.isTransaction)src.exec('ROLLBACK');src.close();}
}
module.exports={mirrorShadow};
