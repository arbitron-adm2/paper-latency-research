'use strict';
const {DatabaseSync}=require('node:sqlite'),zlib=require('node:zlib'),crypto=require('node:crypto');
const schema=`CREATE TABLE IF NOT EXISTS strategy_tape_sessions(id TEXT PRIMARY KEY,started_ms REAL NOT NULL,last_ms REAL NOT NULL,status TEXT NOT NULL,source TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS strategy_tape_chunks(id INTEGER PRIMARY KEY,session_id TEXT NOT NULL,boot_id TEXT NOT NULL,first_sequence INTEGER NOT NULL,last_sequence INTEGER NOT NULL,first_recv_ms REAL NOT NULL,last_recv_ms REAL NOT NULL,recorded_ms REAL NOT NULL,event_n INTEGER NOT NULL,sha256 TEXT NOT NULL,data_z BLOB NOT NULL,UNIQUE(boot_id,first_sequence));
CREATE INDEX IF NOT EXISTS strategy_tape_time ON strategy_tape_chunks(last_recv_ms,first_recv_ms);
CREATE TABLE IF NOT EXISTS strategy_tape_gaps(id INTEGER PRIMARY KEY,session_id TEXT NOT NULL,at_ms REAL NOT NULL,reason TEXT NOT NULL,details_json TEXT NOT NULL);`;
class TapeStore{
 constructor(file,{now=Date.now()}={}){this.db=new DatabaseSync(file);this.db.exec('PRAGMA busy_timeout=25;PRAGMA synchronous=NORMAL;PRAGMA wal_autocheckpoint=0;');this.db.exec(schema);this.id=crypto.randomUUID();this.started=now;this.db.prepare('INSERT INTO strategy_tape_sessions VALUES(?,?,?,?,?)').run(this.id,now,now,'RUNNING','dashboard canonical RAM feed; no external WS');this.chunks=0;this.events=0;}
 append(rows,boot,now=Date.now()){
  if(!rows.length)return;const bytes=Buffer.from(JSON.stringify(rows)),hash=crypto.createHash('sha256').update(bytes).digest('hex'),compressed=zlib.deflateSync(bytes,{level:1});
  this.db.exec('BEGIN IMMEDIATE');try{const r=this.db.prepare('INSERT OR IGNORE INTO strategy_tape_chunks(session_id,boot_id,first_sequence,last_sequence,first_recv_ms,last_recv_ms,recorded_ms,event_n,sha256,data_z) VALUES(?,?,?,?,?,?,?,?,?,?)').run(this.id,boot,rows[0].sequence,rows.at(-1).sequence,rows[0].recv_ms,rows.at(-1).recv_ms,now,rows.length,hash,compressed);this.db.prepare('UPDATE strategy_tape_sessions SET last_ms=? WHERE id=?').run(now,this.id);this.db.exec('COMMIT');if(r.changes){this.chunks++;this.events+=rows.length;}}catch(e){this.db.exec('ROLLBACK');throw e;}
 }
 gap(reason,details,now=Date.now()){this.db.prepare('INSERT INTO strategy_tape_gaps(session_id,at_ms,reason,details_json) VALUES(?,?,?,?)').run(this.id,now,reason,JSON.stringify(details));}
 close(status='STOPPED'){this.db.prepare('UPDATE strategy_tape_sessions SET status=?,last_ms=? WHERE id=?').run(status,Date.now(),this.id);this.db.close();}
}
module.exports={TapeStore,schema};
