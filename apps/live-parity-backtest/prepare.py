"""Freeze read-only execution labels and causal BTC quotes for an offline parity run."""
import argparse
import hashlib
import json
import re
import sqlite3
import subprocess
import zlib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def dump(path, value):
    with Path(path).open('x') as f:
        json.dump(value, f, ensure_ascii=False, indent=2)


def readonly(path):
    c = sqlite3.connect(Path(path).resolve().as_uri() + '?mode=ro', uri=True, timeout=20)
    c.row_factory = sqlite3.Row
    c.execute('PRAGMA query_only=ON')
    return c


def freeze_sources(out):
    rows = []
    for folder in ('apps/papertrade-shadow', 'apps/strategy-lab', 'apps/paper-queue-latency/src'):
        for src in sorted((ROOT / folder).rglob('*')):
            if not src.is_file() or src.suffix not in ('.cjs', '.js', '.json', '.sql', '.py'):
                continue
            if 'node_modules' in src.parts:
                continue
            data = src.read_bytes()
            dest = out / 'source' / src.relative_to(ROOT)
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(data)
            rows.append({'path': str(src.relative_to(ROOT)), 'sha256': hashlib.sha256(data).hexdigest()})
    dump(out / 'source-manifest.json', rows)


def prepare(out, live_file, start, split, end, warmup, profiles):
    out.mkdir(parents=True, exist_ok=False)
    # The split and algorithms are declared before validation outcomes are computed.
    plan = dict(schemaVersion=1, start=start, split=split, end=end, warmupMs=warmup,
                labelSource=str(live_file), financialMutations=0,
                sizing={'margin': 20, 'leverage': 100, 'initialVirtualCapital': 2300},
                calibration='Only labels CLOSED and observed before split; never validation labels',
                priceModel='past-only HL quote; lag selected on train from 0/250/500/1000/1500/2000 ms',
                variants=['vlad_baseline', 'train_queue_only', 'train_queue_and_observation'],
                limitations=['Retrospective chronological holdout, not an untouched future session',
                             'Conditional execution replay uses recorded decisions; free replay does not',
                             'No demonstrated transfer to any larger notional'])
    dump(out / 'plan.json', plan)
    freeze_sources(out)
    profile_data = profiles.read_bytes()
    (out / 'strategy-profiles.json').write_bytes(profile_data)
    dump(out / 'profile-provenance.json', {'path': str(profiles.resolve()), 'sha256': hashlib.sha256(profile_data).hexdigest()})
    src = Path(live_file).resolve()
    original = src.read_bytes()
    dump(out / 'label-provenance.json', {'path': str(src), 'sha256': hashlib.sha256(original).hexdigest()})
    live = json.loads(original)
    c = readonly(ROOT / 'data/unified/market.sqlite3')
    labels = []
    for x in live:
        r = dict(x)
        r['entry_info'] = json.loads(r['entry_info'] or '{}')
        r['exit_info'] = json.loads(r['exit_info'] or '{}')
        if r.get('position_id'):
            hits = c.execute('SELECT close_evidence FROM bot__btcy2_lag_trades WHERE position_id=? AND status=?',
                             (r['position_id'], 'closed')).fetchall()
            if len(hits) != 1:
                raise ValueError('Nonunique/missing close evidence for label ' + str(r['id']))
            r['close_evidence'] = json.loads(hits[0][0] or '{}')
        labels.append(r)
    # Whitelist log lines only. No environment, keys, request bodies or private runtime is read.
    p = subprocess.Popen(['journalctl', '--user', '-u', 'paper-vchera2-live-20261020.service',
                          '--since', '@' + str(start // 1000), '--until', '@' + str((end + 60000) // 1000),
                          '--output=json', '--no-pager'], stdout=subprocess.PIPE, text=True)
    observed = {}
    patterns = [('entry', re.compile(r'^\[btc-вчера2\] ВХОД LIVE .*?#(\d+) ')),
                ('exit', re.compile(r'^\[btc-вчера2\] ВЫХОД #(\d+) '))]
    for line in p.stdout:
        r = json.loads(line)
        for kind, pattern in patterns:
            m = pattern.search(r.get('MESSAGE', ''))
            if m:
                observed.setdefault(int(m[1]), {}).setdefault(kind, int(r['__REALTIME_TIMESTAMP']) / 1000)
    if p.wait() != 0:
        raise ValueError('JOURNAL_READ_FAILED')
    for r in labels:
        r['observed'] = observed.get(r['id'], {})
    dump(out / 'labels.json', labels)
    cache = sqlite3.connect(out / 'quotes.sqlite')
    cache.executescript('CREATE TABLE quotes(venue TEXT,recv REAL,source REAL,bid REAL,ask REAL);'
                        'CREATE TABLE gaps(at REAL,reason TEXT);')
    counts = {'events': 0, 'hl': 0, 'bf': 0, 'chunks': 0, 'duplicateEvents': 0}
    last = {}; last_boot = None; last_seq = None; last_recv = None; digest = hashlib.sha256()
    batch = []
    chunks = c.execute('SELECT boot_id,sha256,data_z FROM strategy_tape_chunks '
                       'WHERE last_recv_ms>=? AND first_recv_ms<=? ORDER BY id', (start - warmup, end))
    for chunk in chunks:
        data = zlib.decompress(chunk['data_z'])
        if hashlib.sha256(data).hexdigest() != chunk['sha256']:
            raise ValueError('TAPE_CHECKSUM')
        counts['chunks'] += 1
        digest.update(chunk['sha256'].encode())
        for e in json.loads(data):
            t = e['recv_ms']
            if not start - warmup <= t <= end:
                continue
            boot = chunk['boot_id']; seq = e['sequence']
            if seq <= last.get(boot, 0):
                counts['duplicateEvents'] += 1
                continue
            last[boot] = seq
            if last_boot is not None and (boot != last_boot or seq != last_seq + 1):
                cache.execute('INSERT INTO gaps VALUES(?,?)', (t, 'BOOT_OR_SEQUENCE_GAP'))
            if last_recv is not None and t < last_recv:
                raise ValueError('RECEIVE_TIME_REGRESSION')
            last_boot, last_seq, last_recv = boot, seq, t
            counts['events'] += 1
            if e['kind'] == 'market' and e.get('symbol') == 'BTC' and e['venue'] in ('hl', 'bf'):
                q = e['payload']
                if q['frameKind'] != 'quote':
                    continue  # Depth never renews execution quote.
                source = e.get('source_ms')
                if source is not None and source > t + 1000:
                    continue
                batch.append((e['venue'], t, source, float(q['bid']), float(q['ask'])))
                counts[e['venue']] += 1
        if len(batch) > 50000:
            cache.executemany('INSERT INTO quotes VALUES(?,?,?,?,?)', batch); batch.clear()
    cache.executemany('INSERT INTO quotes VALUES(?,?,?,?,?)', batch)
    cache.execute('CREATE INDEX quotes_time ON quotes(venue,recv)')
    cache.commit(); cache.close(); c.close()
    counts['chunkDigest'] = digest.hexdigest()
    counts['observedEntries'] = sum('entry' in r['observed'] for r in labels if r['status'] == 'closed')
    counts['observedExits'] = sum('exit' in r['observed'] for r in labels if r['status'] == 'closed')
    dump(out / 'input-summary.json', counts)
    print(json.dumps(counts))


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--out', type=Path, required=True)
    p.add_argument('--live', type=Path, required=True)
    p.add_argument('--profiles', type=Path, required=True)
    for k in ('start', 'split', 'end'):
        p.add_argument('--' + k, type=int, required=True)
    p.add_argument('--warmup-minutes', type=int, default=35)
    a = p.parse_args()
    if not a.start < a.split < a.end or not 0 <= a.warmup_minutes <= 120:
        p.error('Invalid window')
    prepare(a.out, a.live, a.start, a.split, a.end, a.warmup_minutes * 60000, a.profiles)
