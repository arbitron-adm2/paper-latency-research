import collections
import csv
import datetime as dt
import fcntl
import hashlib
import json
import math
import os
import signal
import shutil
import sqlite3
import time
import zlib
from pathlib import Path

DEFAULT_ROOT = Path(__file__).resolve().parents[1]

SCHEMA = '''
CREATE TABLE IF NOT EXISTS sources(
 id INTEGER PRIMARY KEY, path TEXT UNIQUE NOT NULL, sha256 TEXT,
 format TEXT NOT NULL, dataset TEXT NOT NULL, role TEXT NOT NULL,
 venue_hint TEXT, symbol_hint TEXT, bot_hint TEXT, timezone_rule TEXT,
 expected_rows INTEGER, cursor INTEGER NOT NULL DEFAULT 0,
 complete INTEGER NOT NULL DEFAULT 0, errors INTEGER NOT NULL DEFAULT 0,
 min_ms INTEGER, max_ms INTEGER, last_recv_ms REAL, updated_ms REAL
);
CREATE INDEX IF NOT EXISTS sources_hash ON sources(sha256);
CREATE TABLE IF NOT EXISTS aliases(path TEXT PRIMARY KEY, source_id INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS records(
 id INTEGER PRIMARY KEY, source_id INTEGER NOT NULL, row_no INTEGER NOT NULL,
 ts_ms INTEGER, recv_ms REAL, venue TEXT, symbol TEXT, kind TEXT NOT NULL,
 context_id TEXT, mode TEXT, raw_z BLOB NOT NULL
);
CREATE TABLE IF NOT EXISTS ticks(
 record_id INTEGER NOT NULL, sub INTEGER NOT NULL, source_id INTEGER NOT NULL,
 venue TEXT NOT NULL, symbol TEXT, kind TEXT NOT NULL, ts_ms INTEGER, recv_ms REAL,
 bid REAL, ask REAL, bid_size REAL, ask_size REAL, price REAL, size REAL,
 mid REAL, basis REAL, cushion REAL, context_id TEXT,
 PRIMARY KEY(record_id,sub)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS ticks_market_time ON ticks(venue,symbol,ts_ms);
CREATE VIEW IF NOT EXISTS source_coverage AS
 SELECT dataset,role,COUNT(*) files,SUM(cursor) rows,SUM(complete) completed_files,
 SUM(errors) errors,MIN(min_ms) first_ms,MAX(max_ms) last_ms FROM sources GROUP BY dataset,role;
CREATE VIEW IF NOT EXISTS backtest_ticks AS
 SELECT t.*,s.dataset,s.role FROM ticks t JOIN sources s ON s.id=t.source_id
 WHERE s.role IN ('market','live','legacy','flow');
CREATE VIEW IF NOT EXISTS trade_window_ticks AS
 SELECT t.*,s.dataset,s.role FROM ticks t JOIN sources s ON s.id=t.source_id
 WHERE s.role IN ('trade_window','selected_success');
CREATE VIEW IF NOT EXISTS trade_records AS
 SELECT r.*,s.dataset,s.bot_hint FROM records r JOIN sources s ON s.id=r.source_id
 WHERE r.kind='trade_export';
CREATE VIEW IF NOT EXISTS market_ticks AS
 SELECT t.*,s.dataset,s.role,
 CASE WHEN t.venue IN ('hl','paper') THEN 'USDC'
      WHEN t.venue LIKE 'binance%' AND s.path LIKE '%binance_usdc%' THEN 'USDC'
      WHEN t.venue LIKE 'binance%' THEN 'USDT' ELSE NULL END AS quote_asset
 FROM ticks t JOIN sources s ON s.id=t.source_id;
'''


def connect(path, readonly=False):
    path = Path(path)
    if readonly:
        c = sqlite3.connect(path.resolve().as_uri() + '?mode=ro', uri=True, timeout=30)
    else:
        path.parent.mkdir(parents=True, exist_ok=True)
        c = sqlite3.connect(path, timeout=30)
        c.execute('PRAGMA journal_mode=WAL')
        c.execute('PRAGMA synchronous=NORMAL')
        c.execute('PRAGMA cache_size=-65536')
        c.execute('PRAGMA wal_autocheckpoint=8192')
        c.execute('PRAGMA journal_size_limit=134217728')
        c.executescript(SCHEMA)
        c.commit()
    c.row_factory = sqlite3.Row
    c.create_function('raw_json', 1, lambda b: zlib.decompress(b).decode('utf-8'), deterministic=True)
    return c


def number(x):
    try:
        v = float(str(x).replace('\u00a0', '').replace(' ', '').replace(',', '.'))
        return v if math.isfinite(v) else None
    except (TypeError, ValueError):
        return None


def timestamp(x, timezone_rule='unknown'):
    n = number(x)
    if n is not None:
        if n > 1e17: n /= 1e6
        elif n > 1e14: n /= 1000
        elif 1e9 < n < 1e11: n *= 1000
        return int(n) if 1e11 < n < 1e14 else None
    if not isinstance(x, str) or not x:
        return None
    try:
        d = dt.datetime.fromisoformat(x.replace('Z', '+00:00'))
    except ValueError:
        return None
    if d.tzinfo is None:
        if 'UTC+02' in timezone_rule or 'UTC+2' in timezone_rule:
            d = d.replace(tzinfo=dt.timezone(dt.timedelta(hours=2)))
        else:
            return None
    return int(d.timestamp() * 1000)


def symbol(x):
    if x is None or isinstance(x, (bool, dict, list)): return None
    s = str(x).upper()
    for suffix in ['USDT', 'USDC', '-PERP']:
        if s.endswith(suffix): s = s[:-len(suffix)]
    return s


def first(d, *names):
    return next((d[k] for k in names if d.get(k) not in [None, '']), None)


def decode(raw, src, with_clock=False):
    """Return lossless source-row metadata and a normalized list of quote/trade observations."""
    obj = json.loads(raw)
    recv = None
    ts = None
    venue = src['venue_hint'] or 'cross_venue'
    sym = src['symbol_hint']
    kind = src['dataset']
    context = None
    mode = None
    ticks = []

    def tick(v, s, k, t=None, bid=None, ask=None, bs=None, az=None, px=None, sz=None, mid=None, basis=None, cushion=None):
        b, a = number(bid), number(ask)
        m = number(mid) if mid is not None else (b + a) / 2 if b is not None and a is not None else None
        ticks.append((v, symbol(s), k, timestamp(t) if t is not None else ts, recv,
                      b, a, number(bs), number(az), number(px), number(sz), m,
                      number(basis), number(cushion), context))

    if src['format'] == 'csv':
        ts = next((v for v in (timestamp(obj.get(k), src['timezone_rule']) for k in
                  ['time_ms', 'время мс', 'time_utc', 'время (UTC+2)', 'время', 'closed_utc', 'закрыто', 'opened_utc', 'decided_utc']) if v is not None), None)
        sym = symbol(first(obj, 'монета', 'symbol', 'instrument') or sym)
        cid = first(obj, 'trade_id', 'сделка', 'id', 'номер')
        context = str(cid) if cid is not None else None
        raw_mode = first(obj, 'live', 'режим')
        mode = str(raw_mode) if raw_mode is not None else 'export_label_LIVE' if 'sdelki-live' in Path(src['path']).name else None
        kind = 'tick_window' if src['role'] in ['trade_window', 'selected_success'] and ('ticks' in Path(src['path']).name or 'tiki-' in Path(src['path']).name) else src['dataset']
        if kind == 'tick_window':
            hl = first(obj, 'hl_mid', 'HL', 'hl', 'HL mid', 'hl mid', 'цена HL', 'HL: цена')
            bf = first(obj, 'binance_fut', 'Binance Futures', 'BinF', 'bf', 'цена Binance', 'Binance фьюч: цена', 'Binance фьюч')
            # Russian full export field names are also resolved by their explicit header labels.
            for k, v in obj.items():
                low = k.lower()
                if hl is None and 'hl' in low and ('цена' in low or 'mid' in low): hl = v
                if bf is None and ('binance' in low or 'фьюч' in low) and ('цена' in low or 'mid' in low): bf = v
            if hl is not None: tick('hl', sym, 'snapshot', mid=hl)
            if bf is not None: tick('binance_futures', sym, 'snapshot', mid=bf)
    elif isinstance(obj, list):
        ts = timestamp(obj[0]) if obj else None
        if src['dataset'] == 'market_snapshot':
            tick('hl', sym, 'snapshot', mid=obj[1], cushion=obj[3])
            tick('binance_futures', sym, 'snapshot', mid=obj[2], cushion=obj[3])
        kind = src['dataset']
    elif isinstance(obj, dict) and src['dataset'] == 'eth_flow':
        ts = timestamp(obj.get('t'))
        kind = 'flow_' + str(obj.get('k'))
        if obj.get('k') == 'q':
            tick('hl', 'ETH', 'bbo', bid=obj.get('hb'), ask=obj.get('ha'))
            tick('binance_futures', 'ETH', 'bbo', bid=obj.get('bb'), ask=obj.get('ba'))
        else:
            tick('binance_futures', 'ETH', 'flow', px=obj.get('px'), sz=obj.get('n'))
    elif isinstance(obj, dict) and src['dataset'] == 'platform_trades':
        ts = timestamp(obj.get('ts'))
        recv = timestamp(obj.get('rec'))
        sym = symbol(obj.get('instrument')) or {0:'BTC',1:'ETH'}.get(obj.get('instrumentId'))
        kind = 'paper_' + str(obj.get('kind', 'trade'))
        venue = 'paper'
        tick('paper', sym, kind, px=first(obj, 'exit', 'entry'))
    elif isinstance(obj, dict) and 'recv_ms' in obj and 'data' in obj:
        recv = number(obj['recv_ms'])
        data = obj['data']
        if src['venue_hint']=='research':
            kind=data.get('kind','research_event');ts=timestamp(data.get('event_ms'))
            context=data.get('intent_id') or data.get('position_id');mode=data.get('mode')
        elif src['venue_hint'] in ['hl', 'hl_agg', 'hl_fast']:
            venue = 'hl'
            kind = str(data.get('channel', 'message'))
            d = data.get('data', {})
            if kind in ['bbo', 'l2Book'] and isinstance(d, dict):
                sym = symbol(d.get('coin')); ts = timestamp(d.get('time'))
                if kind == 'bbo':
                    levels = d.get('bbo') or [None, None]
                    b, a = levels if len(levels) == 2 else [None, None]
                else:
                    lv = d.get('levels') or [[], []]
                    b = lv[0][0] if len(lv) > 0 and lv[0] else None
                    a = lv[1][0] if len(lv) > 1 and lv[1] else None
                tick('hl', sym, 'depth_agg' if src['venue_hint'] == 'hl_agg' else 'depth_fast' if src['venue_hint'] == 'hl_fast' and kind == 'l2Book' else 'bbo' if kind == 'bbo' else 'depth',
                     ts, bid=(b or {}).get('px'), ask=(a or {}).get('px'), bs=(b or {}).get('sz'), az=(a or {}).get('sz'))
            elif kind == 'trades' and isinstance(d, list):
                for t in d:
                    st = timestamp(t.get('time')); ts = max(ts or 0, st or 0) or None
                    sym = symbol(t.get('coin'))
                    tick('hl', sym, 'trade', st, px=t.get('px'), sz=t.get('sz'))
        elif src['venue_hint'] in ['binance_futures', 'binance_spot']:
            venue = src['venue_hint']
            d = data.get('data', data)
            stream = data.get('stream', '')
            sym = symbol(d.get('s') or stream.split('@')[0] or None); ts = timestamp(first(d, 'T', 'E'))
            kind = d.get('e') or ('bookTicker' if '@bookTicker' in stream else 'depthUpdate' if '@depth' in stream else 'message')
            if kind == 'bookTicker':
                tick(venue, sym, 'bbo', ts, d.get('b'), d.get('a'), d.get('B'), d.get('A'))
            elif kind == 'depthUpdate':
                b, a = d.get('b') or d.get('bids') or [], d.get('a') or d.get('asks') or []
                tick(venue, sym, 'depth', ts, b[0][0] if b else None, a[0][0] if a else None,
                     b[0][1] if b else None, a[0][1] if a else None)
            elif kind in ['aggTrade', 'trade']:
                tick(venue, sym, 'trade', ts, px=d.get('p'), sz=d.get('q'))
        elif src['venue_hint'] == 'paper':
            venue = 'paper'; kind = 'message'
            pubs = list(data.get('subscribe', {}).get('publications', []))
            if data.get('push', {}).get('pub'): pubs.append(data['push']['pub'])
            for pub in pubs:
                d = pub.get('data')
                if isinstance(d, list):
                    kind = 'paper_trades'
                    for t in d:
                        if not isinstance(t, dict): continue
                        st = timestamp(t.get('ts')); ts = max(ts or 0, st or 0) or None
                        instrument = t.get('instrument')
                        if instrument is None: instrument = {0: 'BTC', 1: 'ETH'}.get(t.get('instrumentId'))
                        tick('paper', instrument, 'paper_' + str(t.get('kind', 'trade')), st, px=first(t, 'exit', 'entry'))
                elif isinstance(d, dict):
                    snap = d.get('snapshot', d); ts = timestamp(snap.get('ts'))
                    kind = 'pressure' if 'chain' in snap else 'trading'
        else:
            venue = src['venue_hint']; kind = 'control'
    elif isinstance(obj, dict):
        ts = timestamp(first(obj, 't', 'ts', 'time_ms'))
    result = ((ts, recv, venue, sym, kind, context, mode), ticks)
    return (*result, obj) if with_clock else result


class Reader:
    def __init__(self, src):
        self.src = src
        self.path = Path(src['path'])
        self.line_no = 0
        self.pending = collections.deque()
        self.buf = b''
        self.eof = False
        self.z = zlib.decompressobj(31) if src['format'] == 'gzip' else None
        self.f = self.path.open('rb') if self.z else self.path.open('r', encoding='utf-8-sig', newline='')
        self.csv = csv.DictReader(self.f, delimiter=src.get('delimiter', ',')) if src['format'] == 'csv' else None
        for _ in range(src['cursor']):
            if self.next_raw() is None:
                raise RuntimeError('Source shrank before checkpoint: ' + str(self.path))

    def next_raw(self):
        if self.csv:
            try: row = next(self.csv)
            except StopIteration: self.eof = True; return None
            self.line_no += 1
            return json.dumps(row, ensure_ascii=False, separators=(',', ':')).encode()
        if not self.z:
            while True:
                line = self.f.readline()
                if not line: self.eof = True; return None
                if line.strip(): self.line_no += 1; return line.rstrip('\r\n').encode()
        while not self.pending:
            block = self.f.read(65536)
            if not block:
                self.eof = self.z.eof
                return None
            data = self.buf + self.z.decompress(block)
            lines = data.split(b'\n'); self.buf = lines.pop()
            self.pending.extend(x for x in lines if x.strip())
            if self.z.eof and self.buf.strip():
                raise RuntimeError('Missing final newline in completed gzip')
        self.line_no += 1
        return self.pending.popleft()

    def close(self): self.f.close()


def hashed(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as f:
        for b in iter(lambda: f.read(1024 * 1024), b''): h.update(b)
    return h.hexdigest()


class Warehouse:
    def __init__(self, root, database=None):
        self.root = Path(root)
        self.path = Path(database or self.root / 'data/unified/market.sqlite3')
        self.db = connect(self.path)
        from .research_schema import init
        init(self.db)
        self.readers = {}
        self.descriptors = {}
        self.history = collections.deque()
        self.stopped = False
        self.last_discovery = 0
        self.last_status = 0
        self.last_commit = None
        self.total_new = 0
        self.parse_errors = 0
        self.supplement = None
        self.live_source_ids = {}
        self.capture_sessions = {}

    def register(self, desc):
        desc['path'] = str(Path(desc['path']).resolve())
        row = self.db.execute('SELECT * FROM sources WHERE path=?', (desc['path'],)).fetchone()
        if row is None and desc.get('sha256'):
            row = self.db.execute('SELECT * FROM sources WHERE sha256=? LIMIT 1', (desc['sha256'],)).fetchone()
        if row is None:
            keys = ['path', 'sha256', 'format', 'dataset', 'role', 'venue_hint', 'symbol_hint', 'bot_hint', 'timezone_rule', 'expected_rows']
            self.db.execute('INSERT INTO sources(' + ','.join(keys) + ') VALUES(' + ','.join('?' for _ in keys) + ')', [desc.get(k) for k in keys])
            row = self.db.execute('SELECT * FROM sources WHERE path=?', (desc['path'],)).fetchone()
        else:
            if desc.get('sha256') and row['sha256'] and desc['sha256'] != row['sha256']:
                raise RuntimeError('Immutable input hash changed: ' + desc['path'])
            self.db.execute('UPDATE sources SET expected_rows=COALESCE(?,expected_rows) WHERE id=?', (desc.get('expected_rows'), row['id']))
            if row['path'] == desc['path']:
                self.db.execute('''UPDATE sources SET dataset=?,role=?,
                    venue_hint=COALESCE(?,venue_hint),symbol_hint=COALESCE(?,symbol_hint),
                    bot_hint=COALESCE(?,bot_hint),timezone_rule=COALESCE(?,timezone_rule) WHERE id=?''',
                    (desc['dataset'], desc['role'], desc.get('venue_hint'), desc.get('symbol_hint'),
                     desc.get('bot_hint'), desc.get('timezone_rule'), row['id']))
        self.db.execute('INSERT OR IGNORE INTO aliases VALUES(?,?)', (desc['path'], row['id']))
        s = dict(row); s.update({k: v for k, v in desc.items() if k not in ['path', 'sha256']})
        self.descriptors[s['id']] = s
        self.db.commit()
        return s

    def discover(self):
        profile_path = self.root / 'incoming/vlad/data_profiles.json'
        profiles = json.loads(profile_path.read_text()) if profile_path.exists() else {}
        candidates = profiles.get('csv_profiles', []) + profiles.get('jsonl_profiles', [])
        # Prefer a bot-named provenance alias over the identical loose attachment.
        candidates.sort(key=lambda r: ('vchera2' not in r['path'], 'osnovnoy-bot' not in r['path'], r['path']))
        for r in candidates:
            p = self.root / r['path']; name = p.name.lower(); path = str(p)
            fmt = 'csv' if 'fields' in r else 'jsonl'
            dataset, role = 'metadata', 'context'
            if 'market-' in name: dataset, role = 'market_snapshot', 'market'
            elif 'signals-' in name: dataset, role = 'decision', 'decision'
            elif name.startswith('eth-flow-'): dataset, role = 'eth_flow', 'flow'
            elif name.startswith('pt-trades-'): dataset, role = 'platform_trades', 'platform'
            elif 'ticks' in name or 'tiki-' in name: dataset, role = 'tick_window', 'trade_window'
            elif 'trades' in name or 'sdelki' in name or 'sdelok' in name: dataset, role = 'trade_export', 'trade_history'
            elif 'ocheredi' in name: dataset, role = 'queue_sample', 'queue'
            if 'udachnye' in name: role = 'selected_success'
            s = self.register({'path': path, 'sha256': r['sha256'], 'format': fmt, 'dataset': dataset, 'role': role,
                               'symbol_hint': 'ETH' if 'eth' in name else 'BTC' if any(x in name or x in path for x in ['btc', 'vchera2', 'osnovnoy-bot', '995833']) else None,
                               'bot_hint': 'Вчера 2' if 'vchera2-bot' in path else 'Основной BTC' if 'osnovnoy-bot' in path else None,
                               'timezone_rule': r.get('naive_time_rule', 'epoch'), 'expected_rows': r.get('rows', r.get('nonempty_lines')),
                               'delimiter': r.get('delimiter', ',')})
            if not s['complete'] and s['id'] not in self.history: self.history.append(s['id'])
        for win in ['window1', 'window2', 'window3']:
            base = self.root / 'data/legacy' / win
            for name in ['hl', 'hl_fast', 'hl_agg', 'binance', 'binance_usdc', 'paper', 'control']:
                p = base / (name + '.jsonl')
                if not p.is_file(): continue
                s = self.register({'path': str(p), 'format': 'jsonl', 'dataset': win, 'role': 'legacy',
                                   'venue_hint': 'binance_futures' if name.startswith('binance') else 'hl' if name == 'hl_fast' else name,
                                   'timezone_rule': 'epoch'})
                if not s['complete'] and s['id'] not in self.history: self.history.append(s['id'])
        live = self.root / 'data/live'
        if (live / 'manifest.jsonl').exists():
            for line in (live / 'manifest.jsonl').read_text().splitlines():
                try: r = json.loads(line)
                except json.JSONDecodeError: continue
                s = self.register(self.live_desc(r, expected=True))
                if not s['complete'] and s['id'] not in self.history: self.history.append(s['id'])
        supplemental = self.path.parent/'supplemental/manifest.jsonl'
        if supplemental.exists():
            for line in supplemental.read_text().splitlines():
                try: r = json.loads(line)
                except json.JSONDecodeError: continue
                s = self.register(self.live_desc(r, expected=True))
                if not s['complete'] and s['id'] not in self.history: self.history.append(s['id'])
        telemetry=self.path.parent/'telemetry/manifest.jsonl'
        if telemetry.exists():
            for line in telemetry.read_text().splitlines():
                try:r=json.loads(line)
                except json.JSONDecodeError:continue
                s=self.register(self.live_desc(r,expected=True))
                if not s['complete'] and s['id'] not in self.history:self.history.append(s['id'])
        priority = {'market':0,'flow':1,'trade_history':2,'queue':3,'decision':4,'live':5,'legacy':6,'trade_window':7,'selected_success':8,'context':9}
        self.history = collections.deque(sorted(self.history, key=lambda sid: priority.get(self.descriptors[sid]['role'],10)))
        self.last_discovery = time.monotonic()

    def live_desc(self, r, expected=False):
        research=r.get('venue')=='research'
        return {'path': str(self.root / 'data/live' / r['path']), 'format': 'gzip', 'dataset': 'research_telemetry' if research else 'public_supplement' if 'venue' in r else 'observer', 'role': 'telemetry' if research else 'live',
                'venue_hint': r.get('venue') or ('binance_futures' if r['source'] == 'binance' else r['source']), 'timezone_rule': 'epoch',
                'expected_rows': r['rows'] if expected else None}

    def ingest(self, source_id, limit=6000):
        src = self.descriptors[source_id]
        if src['complete']: return 0
        reader = self.readers.get(source_id)
        if reader is None:
            src['cursor'] = self.db.execute('SELECT cursor FROM sources WHERE id=?', (source_id,)).fetchone()[0]
            reader = self.readers[source_id] = Reader(src)
        records, ticks, clocks, evidence = [], [], [], []
        errors = 0; lo = None; hi = None; last_recv = None
        for _ in range(limit):
            raw = reader.next_raw()
            if raw is None: break
            n = reader.line_no
            try: meta, derived, obj = decode(raw, src, with_clock=True)
            except Exception:
                meta, derived = (None, None, src.get('venue_hint'), src.get('symbol_hint'), 'parse_error', None, None), []
                obj = None
                errors += 1
            ts, recv, venue, sym, kind, context, mode = meta
            rid = (source_id << 40) | n
            records.append((rid, source_id, n, ts, recv, venue, sym, kind, context, mode, zlib.compress(raw, 1)))
            if src['role'] in ['live','telemetry'] and isinstance(obj,dict) and obj.get('recv_ms'):
                cap=obj.get('_capture') or {};connection=str(cap.get('connection','unknown'))
                session_key=(source_id,connection)
                if session_key not in self.capture_sessions:
                    self.db.execute('INSERT OR IGNORE INTO capture_sessions(source_id,connection,boot_id,started_ms) VALUES(?,?,?,?)',
                                    (source_id,connection,cap.get('boot_id'),recv))
                    self.capture_sessions[session_key]=self.db.execute('SELECT id FROM capture_sessions WHERE source_id=? AND connection=?',session_key).fetchone()[0]
                d=obj.get('data') or {};d=d.get('data',d) if isinstance(d,dict) else {}
                if not isinstance(d,dict):d={}
                seq=d.get('a') if d.get('e')=='aggTrade' else d.get('u',d.get('tid'))
                clocks.append((rid,self.capture_sessions[session_key],obj.get('recv_mono_ns'),cap.get('seq'),
                               str(seq) if seq is not None else None,
                               timestamp(d.get('E')),timestamp(d.get('T')),time.time()*1000))
                if src['role']=='telemetry':evidence.append((obj,recv,obj.get('recv_mono_ns')))
                elif venue=='control' and isinstance(obj.get('data'),dict) and obj['data'].get('event'):
                    evidence.append(({'data':{'origin':'market_capture','kind':obj['data']['event'],
                         'event_ms':recv,'precision':'local_wall','payload':obj['data']}},recv,obj.get('recv_mono_ns')))
            for sub, t in enumerate(derived): ticks.append((rid, sub, source_id, *t))
            if ts is not None: lo = ts if lo is None else min(lo, ts); hi = ts if hi is None else max(hi, ts)
            if recv is not None: last_recv = recv if last_recv is None else max(last_recv, recv)
        if records:
            with self.db:
                self.db.executemany('INSERT OR IGNORE INTO records VALUES(' + ','.join('?' for _ in range(11)) + ')', records)
                self.db.executemany('INSERT OR IGNORE INTO ticks VALUES(' + ','.join('?' for _ in range(18)) + ')', ticks)
                # Stamp after acquiring the writer lock and writing the batch, not before
                # a possible SQLite lock wait. This is the final ingest phase, before commit.
                available_ms=time.time()*1000
                self.db.executemany('INSERT OR IGNORE INTO event_clock VALUES(?,?,?,?,?,?,?,?)',
                                    [(*row[:-1],available_ms) for row in clocks])
                if evidence:
                    from .evidence import save_event
                    for args in evidence:save_event(self.db,*args)
                self.db.execute('''UPDATE sources SET cursor=?, errors=errors+?,
                    min_ms=CASE WHEN ? IS NULL THEN min_ms WHEN min_ms IS NULL THEN ? ELSE MIN(min_ms,?) END,
                    max_ms=CASE WHEN ? IS NULL THEN max_ms WHEN max_ms IS NULL THEN ? ELSE MAX(max_ms,?) END,
                    last_recv_ms=CASE WHEN ? IS NULL THEN last_recv_ms WHEN last_recv_ms IS NULL THEN ? ELSE MAX(last_recv_ms,?) END,
                    updated_ms=? WHERE id=?''',
                    (reader.line_no, errors, lo, lo, lo, hi, hi, hi, last_recv, last_recv, last_recv, time.time()*1000, source_id))
            src['cursor'] = reader.line_no; self.total_new += len(records); self.parse_errors += errors; self.last_commit = time.time()*1000
        if reader.eof:
            expected = src.get('expected_rows')
            if expected is not None and reader.line_no != expected:
                raise RuntimeError(f'Expected {expected} rows, read {reader.line_no}: {src["path"]}')
            if src['format'] != 'gzip' or reader.z.eof:
                h = hashed(src['path'])
                if src.get('sha256') and h != src['sha256']:
                    raise RuntimeError('Immutable source failed SHA256 at EOF: ' + src['path'])
                with self.db: self.db.execute('UPDATE sources SET complete=1,sha256=?,updated_ms=? WHERE id=?', (h, time.time()*1000, source_id))
                src['complete'] = 1; reader.close(); self.readers.pop(source_id, None)
        return len(records)

    def live_sources(self):
        p = self.root / 'data/live/status.json'
        if not p.exists(): return []
        state = json.loads(p.read_text())
        chunks = state.get('active_chunks', [])
        q = self.path.parent/'supplemental/status.json'
        if q.exists(): chunks += json.loads(q.read_text()).get('active_chunks', [])
        q=self.path.parent/'telemetry/status.json'
        if q.exists():chunks+=json.loads(q.read_text()).get('active_chunks',[])
        active = []
        for r in chunks:
            desc = self.live_desc(r)
            path = desc['path']
            if path not in self.live_source_ids:
                self.live_source_ids[path] = self.register(desc)['id']
            active.append(self.live_source_ids[path])
        return active

    def status(self):
        data = [dict(r) for r in self.db.execute('SELECT * FROM source_coverage')]
        live = self.db.execute("SELECT venue_hint,MAX(last_recv_ms) last_recv_ms,SUM(cursor) rows FROM sources WHERE role='live' GROUP BY venue_hint").fetchall()
        s = {'now_ms': time.time()*1000, 'pid': os.getpid(), 'database': str(self.path), 'new_rows_this_process': self.total_new,
             'last_commit_ms': self.last_commit, 'coverage': data, 'live': [dict(r) for r in live],
             'parse_errors_this_process': self.parse_errors, 'pending_files': self.db.execute('SELECT COUNT(*) FROM sources WHERE complete=0').fetchone()[0],
             'database_bytes': self.path.stat().st_size, 'mode': 'read_only_sources_local_sql_writer'}
        p = self.path.parent / 'status.json'; tmp = p.with_suffix('.new'); tmp.write_text(json.dumps(s, ensure_ascii=False, indent=2)); tmp.replace(p)
        return s

    def serve(self, once=False):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        lock = (self.path.parent / 'writer.lock').open('a')
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        for sig in [signal.SIGTERM, signal.SIGINT]: signal.signal(sig, lambda *_: setattr(self, 'stopped', True))
        if not once:
            from .supplement import Supplement
            self.supplement = Supplement(self.path.parent/'supplemental')
            self.supplement.start()
        self.discover()
        try:
            while not self.stopped:
                if shutil.disk_usage(self.path.parent).free < 8 * 1024**3:
                    print('Database ingest stopped: disk reserve below 8 GiB; source capture unchanged', flush=True)
                    break
                if time.monotonic() - self.last_discovery > 30: self.discover()
                active = self.live_sources()
                n = 0
                for source_id in active: n += self.ingest(source_id, 12000)
                if self.history:
                    sid = self.history[0]
                    if sid not in active:
                        n += self.ingest(sid, 6000)
                    if self.descriptors[sid]['complete'] or sid in active: self.history.popleft()
                if time.monotonic() - self.last_status > 5:
                    s = self.status(); self.last_status = time.monotonic()
                    print(json.dumps({'rows': sum(x['rows'] for x in s['coverage']), 'pending': s['pending_files'], 'parse_errors': s['parse_errors_this_process']}), flush=True)
                if once and not self.history: break
                if not n: time.sleep(.05)
        finally:
            if self.supplement: self.supplement.close()
            self.status()
            for r in self.readers.values(): r.close()
            self.db.close()
