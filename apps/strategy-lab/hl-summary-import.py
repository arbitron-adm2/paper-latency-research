"""Idempotent derived import into the existing warehouse; never invent BBO/L2."""
import argparse, csv, hashlib, json, math, sqlite3, time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
FIELDS = ['время (локальное)', 'время мс', 'цена HL', 'перевес стакана HL % (плюс = покупатели)', 'покупки у цены HL $', 'продажи у цены HL $', 'крупнейшая заявка HL $', 'её сторона', 'расстояние от цены $']
SCHEMA = '''
CREATE TABLE IF NOT EXISTS strategy_hl_summary_imports(
 source_id INTEGER PRIMARY KEY,sha256 TEXT NOT NULL,rows INTEGER NOT NULL,status TEXT NOT NULL,updated_ms INTEGER NOT NULL,policy_json TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS strategy_hl_book_summaries(
 record_id INTEGER PRIMARY KEY,source_id INTEGER NOT NULL,row_no INTEGER NOT NULL,at_ms INTEGER NOT NULL,
 mid REAL NOT NULL,imbalance_pct REAL,bid_near_usd REAL,ask_near_usd REAL,wall_usd REAL,wall_side TEXT,wall_distance_usd REAL,
 FOREIGN KEY(record_id) REFERENCES records(id));
CREATE INDEX IF NOT EXISTS strategy_hl_summary_time ON strategy_hl_book_summaries(at_ms,source_id);
'''

def numeric(value):
    if value in ('', None): return None
    x = float(str(value).replace(',', '.'))
    if not math.isfinite(x): raise ValueError('Non-finite summary field')
    return x

def parse_row(row, source_id, line):
    t = numeric(row[FIELDS[1]]); mid = numeric(row[FIELDS[2]])
    if t is None or t != int(t) or not 10**12 < t < 10**14 or mid is None or mid <= 0: raise ValueError('Invalid summary clock/price')
    imb, bid, ask, wall, dist = [numeric(row[k]) for k in [FIELDS[3], FIELDS[4], FIELDS[5], FIELDS[6], FIELDS[8]]]
    if imb is not None and not -100 <= imb <= 100: raise ValueError('Invalid imbalance')
    if any(x is not None and x < 0 for x in [bid, ask, wall, dist]): raise ValueError('Negative depth field')
    side = {'покупка': 'bid', 'продажа': 'ask', '': None}.get(row[FIELDS[7]])
    if row[FIELDS[7]] not in ['покупка', 'продажа', '']: raise ValueError('Unknown wall side')
    if (wall is None) != (side is None) or (wall is None) != (dist is None): raise ValueError('Partial wall')
    return ((source_id << 40) | line, source_id, line, int(t), mid, imb, bid, ask, wall, side, dist)

def import_file(c, p, source, expected_sha, expected_rows):
    digest = hashlib.sha256(p.read_bytes()).hexdigest()
    if digest != expected_sha or digest != source['sha256']: raise ValueError('Immutable source SHA mismatch')
    policy = json.dumps({'id': 'vlad-hl-summary-v1', 'clock': 'CSV epoch ms; original exchange receive time unknown',
        'dataClass': 'external derived summary, not raw L2/BBO', 'emptyWall': 'UNKNOWN, not zero', 'rawPreserved': True}, ensure_ascii=False)
    previous = c.execute('SELECT sha256,rows,status FROM strategy_hl_summary_imports WHERE source_id=?', (source['id'],)).fetchone()
    if previous and previous[0] != digest: raise ValueError('Derived source identity changed')
    if previous and previous[1] == expected_rows and previous[2] == 'COMPLETE':
        actual = c.execute('SELECT count(*) FROM strategy_hl_book_summaries WHERE record_id BETWEEN ? AND ?', (source['id'] << 40, ((source['id']+1) << 40)-1)).fetchone()[0]
        if actual != expected_rows: raise ValueError('Previously completed import count changed')
        return {'sourceId':source['id'],'sha256':digest,'rows':actual,'status':'COMPLETE','replayed':True,'rawRowsUntouched':True,'bboCreated':False}
    batch = []; count = 0; last = None
    with p.open(encoding='utf-8-sig', newline='') as f:
        reader = csv.DictReader(f, delimiter=';')
        if reader.fieldnames != FIELDS: raise ValueError('Unexpected HL summary schema')
        for line, row in enumerate(reader, 1):
            item = parse_row(row, source['id'], line)
            if last is not None and item[3] <= last: raise ValueError('Summary duplicate/regressing time')
            last = item[3]; count += 1; batch.append(item)
            if len(batch) >= 1000:
                with c: c.executemany('INSERT OR IGNORE INTO strategy_hl_book_summaries VALUES(?,?,?,?,?,?,?,?,?,?,?)', batch)
                batch.clear()
        if batch:
            with c: c.executemany('INSERT OR IGNORE INTO strategy_hl_book_summaries VALUES(?,?,?,?,?,?,?,?,?,?,?)', batch)
    if count != expected_rows: raise ValueError('CSV expected row mismatch')
    actual = c.execute('SELECT count(*) FROM strategy_hl_book_summaries WHERE record_id BETWEEN ? AND ?', (source['id'] << 40, ((source['id']+1) << 40)-1)).fetchone()[0]
    if actual != count: raise ValueError('Derived row count mismatch')
    with c:
        c.execute('INSERT INTO strategy_hl_summary_imports VALUES(?,?,?,?,?,?) ON CONFLICT(source_id) DO UPDATE SET rows=excluded.rows,status=excluded.status,updated_ms=excluded.updated_ms',
                  (source['id'], digest, count, 'COMPLETE', int(time.time()*1000), policy))
    return {'sourceId': source['id'], 'sha256': digest, 'rows': count, 'status': 'COMPLETE', 'rawRowsUntouched': True, 'bboCreated': False}

def main():
    p=argparse.ArgumentParser();p.add_argument('--out',type=Path,required=True);args=p.parse_args()
    manifest=json.loads((ROOT/'incoming/vlad/download_manifest.json').read_text())
    profiles={r['path']:r for r in json.loads((ROOT/'incoming/vlad/data_profiles.json').read_text())['csv_profiles']}
    files=[r for r in manifest['documents'] if r['message_id'] in [998700,998701]]
    if len(files)!=2: raise ValueError('Expected both immutable HL attachments')
    c=sqlite3.connect(ROOT/'data/unified/market.sqlite3',timeout=5);c.row_factory=sqlite3.Row
    c.execute('PRAGMA journal_mode=WAL');c.execute('PRAGMA busy_timeout=5000');c.execute('PRAGMA cache_size=-8192');c.executescript(SCHEMA)
    result=[]
    for r in files:
        source=c.execute('SELECT id,sha256,cursor,complete FROM sources WHERE path=?',(str(ROOT/r['path']),)).fetchone()
        if not source or not source['complete'] or source['cursor']!=profiles[r['path']]['rows']: raise ValueError('Raw source must already be completely imported')
        result.append(import_file(c,ROOT/r['path'],dict(source),r['sha256'],profiles[r['path']]['rows']))
    c.close();args.out.write_text(json.dumps(result,ensure_ascii=False,indent=2));print(json.dumps(result))
if __name__=='__main__':main()
