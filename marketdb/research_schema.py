"""Append-only research evidence and reproducible detector runs in the existing DB."""
SCHEMA = '''
CREATE TABLE IF NOT EXISTS capture_sessions(id INTEGER PRIMARY KEY,source_id INTEGER NOT NULL,
 connection TEXT NOT NULL,boot_id TEXT,started_ms REAL,UNIQUE(source_id,connection));
CREATE TABLE IF NOT EXISTS event_clock(record_id INTEGER PRIMARY KEY,session_id INTEGER,
 recv_mono_ns INTEGER,local_seq INTEGER,provider_seq TEXT,source_event_ms REAL,
 source_trade_ms REAL,ingested_ms REAL);
CREATE INDEX IF NOT EXISTS event_clock_available ON event_clock(ingested_ms,record_id);
CREATE TABLE IF NOT EXISTS research_events(id INTEGER PRIMARY KEY,event_key TEXT UNIQUE NOT NULL,
 origin TEXT NOT NULL,kind TEXT NOT NULL,event_ms REAL,observed_ms REAL NOT NULL,
 recv_mono_ns INTEGER,wallet TEXT,intent_id TEXT,position_id TEXT,mode TEXT,precision TEXT,raw_z BLOB NOT NULL);
CREATE INDEX IF NOT EXISTS research_events_time ON research_events(observed_ms,id);
CREATE INDEX IF NOT EXISTS research_events_position ON research_events(position_id,event_ms);
CREATE INDEX IF NOT EXISTS research_events_intent ON research_events(intent_id,event_ms);
CREATE TABLE IF NOT EXISTS execution_facts(event_id INTEGER PRIMARY KEY,position_id TEXT,kind TEXT,
 source_ms REAL,observed_ms REAL,intent_id TEXT,instrument TEXT,side TEXT,price REAL,margin_raw TEXT,
 pnl_raw TEXT,fee_raw TEXT,paper_raw TEXT,funding_raw TEXT,tx_hash TEXT,block_number INTEGER,source TEXT);
CREATE TABLE IF NOT EXISTS detector_runs(id TEXT PRIMARY KEY,mode TEXT NOT NULL,created_ms REAL NOT NULL,
 config_json TEXT NOT NULL,config_sha256 TEXT NOT NULL,code_sha256 TEXT NOT NULL,start_ms REAL,end_ms REAL,
 status TEXT NOT NULL,summary_json TEXT);
CREATE TABLE IF NOT EXISTS detector_signals(id INTEGER PRIMARY KEY,run_id TEXT NOT NULL,event_key TEXT NOT NULL,
 recv_ms REAL NOT NULL,symbol TEXT NOT NULL,side TEXT NOT NULL,strength_bps REAL NOT NULL,
 features_json TEXT NOT NULL,UNIQUE(run_id,event_key));
CREATE INDEX IF NOT EXISTS detector_signals_run ON detector_signals(run_id,recv_ms);
CREATE TABLE IF NOT EXISTS detector_outcomes(run_id TEXT,signal_key TEXT,horizon_ms INTEGER,
 observed_ms REAL,markout_bps REAL,adverse_bps REAL,favorable_bps REAL,status TEXT,details_json TEXT,
 PRIMARY KEY(run_id,signal_key,horizon_ms)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS detector_checkpoints(run_id TEXT,source_id INTEGER,row_no INTEGER,
 PRIMARY KEY(run_id,source_id)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS research_state(name TEXT PRIMARY KEY,json TEXT NOT NULL,updated_ms REAL NOT NULL);
'''


def init(c):
    c.executescript(SCHEMA)
    c.commit()
