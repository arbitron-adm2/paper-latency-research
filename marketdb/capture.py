"""Record the new capture epoch into the canonical database, without resurrecting deleted history."""
import argparse,hashlib,json,time,zlib
from pathlib import Path
from .core import Warehouse,DEFAULT_ROOT

class CaptureWarehouse(Warehouse):
    def __init__(self,root,database=None):
        root=Path(root);self.epoch_file=root/'data/unified/capture-epoch.json'
        if not self.epoch_file.exists():self.epoch_file=root/'data/unified/capture-20261022.json'
        if not self.epoch_file.exists():raise RuntimeError('Capture epoch must be initialized explicitly before startup')
        self.start_ms=json.loads(self.epoch_file.read_text())['start_ms']
        super().__init__(root,database)
        self.db.executescript('''CREATE TABLE IF NOT EXISTS capture_paper_parameters(
            record_id INTEGER PRIMARY KEY,recv_ms REAL NOT NULL,indexed_block INTEGER,
            exchange_version TEXT,off_hours INTEGER,win_fee_raw TEXT,body_sha256 TEXT NOT NULL);
            CREATE INDEX IF NOT EXISTS capture_parameters_time ON capture_paper_parameters(recv_ms);''')
        self.db.commit()
        for src in self.db.execute("SELECT id FROM sources WHERE venue_hint='paper_parameters'").fetchall():self.parameter_rows(src[0])
    def eligible(self,row):
        return row.get('last_recv_ms',0)>=self.start_ms and Path(self.live_desc(row)['path']).is_file()
    def discover(self):
        for manifest in [self.root/'data/live/manifest.jsonl',self.path.parent/'supplemental/manifest.jsonl']:
            if not manifest.exists():continue
            for line in manifest.read_text().splitlines():
                try:row=json.loads(line)
                except json.JSONDecodeError:continue
                if not self.eligible(row):continue
                src=self.register(self.live_desc(row,expected=True))
                if not src['complete'] and src['id']not in self.history:self.history.append(src['id'])
        self.last_discovery=time.monotonic()
    def live_sources(self):
        active=[]
        for status in [self.root/'data/live/status.json',self.path.parent/'supplemental/status.json']:
            if not status.exists():continue
            state=json.loads(status.read_text())
            for row in state.get('active_chunks',[]):
                if not self.eligible(row):continue
                desc=self.live_desc(row);path=desc['path']
                if path not in self.live_source_ids:self.live_source_ids[path]=self.register(desc)['id']
                active.append(self.live_source_ids[path])
        return active
    def parameter_rows(self,sid):
        rows=self.db.execute('''SELECT id,recv_ms,raw_z FROM records WHERE id>=? AND id<?
            AND id NOT IN(SELECT record_id FROM capture_paper_parameters)''',(sid<<40,(sid+1)<<40)).fetchall()
        with self.db:
            for row in rows:
                obj=json.loads(zlib.decompress(row['raw_z']));body=obj['data']['body']
                digest=hashlib.sha256(json.dumps(body,sort_keys=True,separators=(',',':')).encode()).hexdigest()
                self.db.execute('INSERT OR IGNORE INTO capture_paper_parameters VALUES(?,?,?,?,?,?,?)',
                    (row['id'],row['recv_ms'],body.get('indexedThroughBlock'),body.get('exchange'),
                     int(body['offHours'])if 'offHours'in body else None,body.get('winFeeRateRaw'),digest))
    def ingest(self,source_id,limit=6000):
        n=super().ingest(source_id,limit)
        if n and self.descriptors[source_id].get('venue_hint')=='paper_parameters':self.parameter_rows(source_id)
        return n

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--project-root',type=Path,default=DEFAULT_ROOT)
    parser.add_argument('--database',type=Path);args=parser.parse_args()
    CaptureWarehouse(args.project_root,args.database).serve()
