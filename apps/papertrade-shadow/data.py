"""Bounded public evidence capture and local tape preparation. Standard library only.
Never imports an exchange client, runtime module, credentials or archived scripts.
"""
import argparse
import os
import datetime as dt
import gzip
import hashlib
import json
from pathlib import Path
import sqlite3
import time
import urllib.request
import urllib.error
from urllib.parse import urlsplit
import zlib

ROOT = Path(__file__).resolve().parents[2]
ACCOUNT = os.environ.get('PAPER_WALLET', '0x1111111111111111111111111111111111111111')
BASE = 'https://exchange.papertrade.xyz'
PUBLIC = {'/', '/state/trading', f'/query/accounts/{ACCOUNT}/trades',
          '/assets/wire-csLx47FA.js', '/assets/QueuedIntentLabel-CazFyFmz.js',
          '/assets/focus-CXaE1eE0.js', '/assets/curve-B6R_lh-Q.js',
          '/assets/schemas-6dZMxIO0.js'}


def digest(b):
    return hashlib.sha256(b).hexdigest()


def save(path, value):
    with Path(path).open('x') as f:
        json.dump(value, f, ensure_ascii=False, indent=2)
        f.write('\n')

def validate_output(path):
    resolved = path.resolve()
    if not resolved.is_relative_to((ROOT / 'reports').resolve()):
        raise PermissionError('Output must stay in a new reports directory')
    if resolved.exists():
        raise FileExistsError('Never overwrite an existing run')


def validate_dataset(path):
    if path.resolve() not in [(ROOT / ('data/' + name)).resolve()
                               for name in ['live', 'capture_snapshot', 'capture_archive']]:
        raise PermissionError('Only the existing collector or frozen capture datasets')



class PublicReader:
    def __init__(self):
        self.last = 0

    def get(self, path, method='GET'):
        if method != 'GET' or path not in PUBLIC:
            raise PermissionError('Default deny: unlisted public GET')
        time.sleep(max(0, 1 - (time.monotonic() - self.last)))
        self.last = time.monotonic()
        # No redirect handler: never forwards to an unlisted destination.
        class NoRedirect(urllib.request.HTTPRedirectHandler):
            def redirect_request(self, *args):
                raise PermissionError('Redirect denied')
        opener = urllib.request.build_opener(NoRedirect, urllib.request.ProxyHandler({}))
        request = urllib.request.Request(BASE + path, headers={'User-Agent': 'PaperTrade-shadow-readonly/1'})
        with opener.open(request, timeout=10) as response:
            body = response.read(4_000_001)
            if len(body) > 4_000_000:
                raise ValueError('Public response exceeds bound')
            return body


def capture(out):
    out.mkdir(parents=True, exist_ok=False)
    reader = PublicReader()
    manifest = []
    for path in sorted(PUBLIC):
        name = 'frontend.html' if path == '/' else path.rsplit('/', 1)[-1]
        if path == '/state/trading': name = 'trading.json'
        if path.endswith('/trades'): name = 'account-trades.json'
        record = {'url': BASE + path, 'method': 'GET', 'at_utc': dt.datetime.now(dt.timezone.utc).isoformat()}
        try:
            b = reader.get(path)
            (out / name).write_bytes(b)
            record.update(file=name, sha256=digest(b), bytes=len(b), status='captured')
        except Exception as e:
            record.update(status='unavailable', error=str(e))
            # No retries in this bounded capture; avoids repeated pressure on 429/5xx.
        manifest.append(record)
    save(out / 'manifest.json', manifest)
    print(json.dumps(manifest, indent=2))


def export_rows(out):
    out.mkdir(parents=True, exist_ok=False)
    db = ROOT / 'apps/vchera2/.runtime/vchera2.sqlite'
    con = sqlite3.connect(db.as_uri() + '?mode=ro', uri=True)
    con.execute('PRAGMA query_only=ON')
    con.row_factory = sqlite3.Row
    con.execute('BEGIN')
    columns = ('id,strategy,side,opened_at,closed_at,entry,exit,qty,margin,leverage,status,'
               'reason,position_id,intent_id,live,real_pnl,est_pnl,entry_info,exit_info,close_evidence')
    rows = [dict(r) for r in con.execute('SELECT ' + columns + ' FROM btcy2_lag_trades ORDER BY id')]
    con.rollback()
    con.close()
    # Never export config, authorization, session keys or signed requests.
    save(out / 'canonical-input.json', rows)
    save(out / 'manifest.json', {'source': 'apps/vchera2/.runtime/vchera2.sqlite',
         'read': 'mode=ro; query_only; single read transaction; selected trade columns only',
         'timezone': 'Unix milliseconds UTC', 'rows': len(rows),
         'sha256_export': digest((out / 'canonical-input.json').read_bytes()),
         'snapshot_at_utc': dt.datetime.now(dt.timezone.utc).isoformat()})
    print(json.dumps({'rows': len(rows), 'output': str(out)}))


def raw_rows(path, active=False):
    if not active:
        with gzip.open(path, 'rt') as f:
            for line in f:
                if line.strip(): yield json.loads(line)
    else:
        # Fixed prefix: tolerate incomplete final gzip member/JSON line without reading ahead.
        remaining = path.stat().st_size
        dec = zlib.decompressobj(16 + zlib.MAX_WBITS)
        buf = b''
        with path.open('rb') as f:
            while remaining:
                b = f.read(min(remaining, 1024 * 1024))
                if not b: break
                remaining -= len(b)
                buf += dec.decompress(b)
                parts = buf.split(b'\n')
                buf = parts.pop()
                for line in parts:
                    if line.strip(): yield json.loads(line)


def canonical(row, source, index, attribution=None):
    """Observed BTC/ETH only. Binance venue needs reviewed capture metadata, never ps."""
    data = row.get('data', {})
    if not isinstance(data, dict): return None
    venue, payload, source_time, market = None, None, None, None
    if source in ('hl', 'hl_fast'):
        data = data.get('data', data) if data.get('channel') in ('bbo', 'l2Book') else data
        market = data.get('coin')
        if market not in ('BTC', 'ETH'): return None
        venue = 'hl'
        source_time = data.get('time')
        if isinstance(data.get('bbo'), list) and len(data['bbo']) == 2:
            b, a = data['bbo']
            payload = {'bid': b['px'], 'ask': a['px']}
        elif isinstance(data.get('levels'), list) and len(data['levels']) == 2:
            b, a = data['levels']
            if not b or not a: return None
            payload = {'bid': b[0]['px'], 'ask': a[0]['px'],
                       'bids': [[x['px'], x['sz']] for x in b],
                       'asks': [[x['px'], x['sz']] for x in a]}
    elif source == 'binance':
        stream = data.get('stream', '')
        data = data.get('data', data)
        if data.get('s') not in ('BTCUSDT', 'ETHUSDT'): return None
        market = data['s'][:-4]
        if not stream.lower().startswith(market.lower() + 'usdt@'): return None
        if not attribution or attribution.get('verified') is not True or not attribution.get('evidence'): return None
        parsed = urlsplit(attribution.get('endpoint', ''))
        if parsed.scheme != 'wss' or parsed.username or parsed.password: return None
        venue = {'fstream.binance.com': 'bf', 'stream.binance.com': 'bs',
                 'data-stream.binance.vision': 'bs'}.get(parsed.hostname)
        if venue is None or venue != attribution.get('venue'): return None
        source_time = data.get('E', data.get('T'))
        if 'bids' in data and 'asks' in data:
            b, a = data['bids'], data['asks']
            if not b or not a: return None
            payload = {'bid': b[0][0], 'ask': a[0][0], 'bids': b, 'asks': a}
        elif isinstance(data.get('b'), str) and isinstance(data.get('a'), str):
            payload = {'bid': data['b'], 'ask': data['a']}
        elif isinstance(data.get('b'), list) and isinstance(data.get('a'), list):
            b, a = data['b'], data['a']
            if not b or not a: return None
            payload = {'bid': b[0][0], 'ask': a[0][0], 'bids': b, 'asks': a}
    if payload is None: return None
    payload.update(frameKind='book' if 'bids' in payload else 'quote',
                   instrumentId=0 if market == 'BTC' else 1,
                   priceScale='10' if market == 'BTC' else '100',
                   sourceTimestampKnown=source_time is not None)
    receive = row['recv_ms']
    event = source_time if source_time is not None else receive
    raw = json.dumps(row, sort_keys=True, separators=(',', ':')).encode()
    return {'schema': 1, 'event_ms': event, 'recv_ms': receive, 'source': source,
            'sequence': index, 'market': market, 'account': None, 'kind': 'market',
            'venue': venue, 'payload': payload, 'capture_record_sha256': digest(raw), 'raw_sha256': digest(raw),
            'quality': {'timestamp': 'source' if source_time is not None else 'receive-only',
                        'depth': 'observed' if 'bids' in payload else 'missing',
                        'hash_kind': 'canonical-capture-record-NOT-raw-wire',
                        'venue_attribution': attribution if source == 'binance' else 'explicit HL coin/channel',
                        'execution_price': 'hypothetical HL midpoint, NOT a server fill'}}


def prepare(out, dataset, since, until, venue_map=None):
    if until <= since or until - since > 3600_000: raise ValueError('Window must be 0..3600 seconds')
    out.mkdir(parents=True, exist_ok=False)
    catalog = []
    for line in (dataset / 'manifest.jsonl').read_text().splitlines():
        if line.strip(): catalog.append((json.loads(line), False))
    status_path = dataset / 'status.json'
    if status_path.exists():
        status = json.loads(status_path.read_text())
        save(out / 'collector-status.json', status)
        catalog += [(m, True) for m in status.get('active_chunks', [])]
    events, manifest, seen = [], [], set()
    rejected_ambiguous_venue = 0
    for m, active in catalog:
        if m['source'] not in ('hl', 'hl_fast', 'binance') or m['last_recv_ms'] < since or m['first_recv_ms'] > until: continue
        path = dataset / m['path']
        if not path.resolve().is_relative_to((dataset / 'chunks').resolve()) or path.suffix != '.gz':
            raise PermissionError('Invalid collector chunk path')
        if path in seen: continue
        seen.add(path)
        before = path.stat().st_size
        count = 0
        first = last = None
        prefix = path.read_bytes()[:before]
        if active:
            dec = zlib.decompressobj(16 + zlib.MAX_WBITS)
            decoded = dec.decompress(prefix)
            lines = decoded.split(b'\n')[:-1]
            stream_rows = (json.loads(line) for line in lines if line.strip())
        else:
            stream_rows = (json.loads(line) for line in gzip.decompress(prefix).splitlines() if line.strip())
        for index, row in enumerate(stream_rows):
            if not since <= row['recv_ms'] <= until: continue
            count += 1
            attribution = (venue_map or {}).get(m['source'])
            e = canonical(row, m['source'], index, attribution)
            if e is None and m['source'] == 'binance' and not attribution:
                rejected_ambiguous_venue += 1
            if e:
                events.append(e)
                first = e['recv_ms'] if first is None else min(first, e['recv_ms'])
                last = e['recv_ms'] if last is None else max(last, e['recv_ms'])
        manifest.append({'path': str(path), 'source': m['source'], 'active_prefix': active,
                         'prefix_bytes': before, 'prefix_sha256': digest(prefix),
                         'raw_rows_in_window': count, 'first_quote_recv_ms': first, 'last_quote_recv_ms': last})
    events.sort(key=lambda e: (e['recv_ms'], e['source'], e['sequence']))
    with (out / 'events.jsonl').open('x') as f:
        for event in events: f.write(json.dumps(event, separators=(',', ':')) + '\n')
    save(out / 'manifest.json', {'timezone': 'UTC Unix milliseconds', 'from_ms': since, 'through_ms': until,
         'source_files': manifest, 'events': len(events), 'tape_sha256': digest((out / 'events.jsonl').read_bytes()),
         'depth_events': sum('bids' in e['payload'] for e in events),
         'rejected_ambiguous_venue_rows': rejected_ambiguous_venue,
         'venue_map': venue_map, 'assets': {a: sum(e['market'] == a for e in events) for a in ('BTC', 'ETH')},
         'window_selection': 'bounded diagnostic window; NOT independent validation of edge'})
    print(json.dumps({'events': len(events), 'depth_events': sum('bids' in e['payload'] for e in events)}))


def shadow(out, dataset, seconds):
    if not 1 <= seconds <= 30: raise ValueError('Bounded smoke duration: 1..30 seconds')
    start = time.time() * 1000
    # One finite observation window of the existing collector; no sockets/daemon/supervisor.
    time.sleep(seconds)
    end = time.time() * 1000
    prepare(out, dataset, start, end)
    save(out / 'shadow-window.json', {'mode': 'shadow', 'from_ms': start, 'through_ms': end,
         'seconds': seconds, 'ingestion': 'bounded buffered read of existing collector; sequential causal replay after capture',
         'new_sockets': 0, 'financial_mutations_sent': 0})


def main():
    p = argparse.ArgumentParser(description=__doc__)
    sub = p.add_subparsers(dest='command', required=True)
    a = sub.add_parser('evidence'); a.add_argument('--out', type=Path, required=True)
    a = sub.add_parser('export'); a.add_argument('--out', type=Path, required=True)
    a = sub.add_parser('prepare'); a.add_argument('--out', type=Path, required=True)
    a.add_argument('--dataset', type=Path, default=ROOT / 'data/capture_snapshot')
    a.add_argument('--since-ms', type=float, required=True); a.add_argument('--until-ms', type=float, required=True)
    a.add_argument('--venue-map', type=Path, help='Explicit reviewed source-to-endpoint mapping JSON; missing fails closed')
    a = sub.add_parser('shadow'); a.add_argument('--out', type=Path, required=True)
    a.add_argument('--dataset', type=Path, default=ROOT / 'data/live')
    a.add_argument('--seconds', type=int, required=True)
    args = p.parse_args()
    validate_output(args.out)
    if hasattr(args, 'dataset'): validate_dataset(args.dataset)
    if args.command == 'evidence': capture(args.out)
    elif args.command == 'export': export_rows(args.out)
    elif args.command == 'shadow': shadow(args.out, args.dataset, args.seconds)
    else:
        venue_map = None
        if args.venue_map:
            if args.venue_map.suffix != '.json' or '.runtime' in args.venue_map.parts: raise PermissionError('Research JSON metadata only')
            venue_map = json.loads(args.venue_map.read_text())
        prepare(args.out, args.dataset, args.since_ms, args.until_ms, venue_map)


if __name__ == '__main__': main()
