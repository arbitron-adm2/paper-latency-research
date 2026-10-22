"""Offline views for the BTC long adapter: own-data parity and fresh warmup."""
import argparse
import hashlib
import json
import math
from pathlib import Path
import sqlite3
import subprocess
import time

import numpy as np

from prepare import ROOT, canonical, save_json
from align_books import align
from causal_entries import features, signal

FIELDS = ('t', 'hl', 'bf', 'hl_age', 'bf_age', 'hl_low', 'hl_high', 'hl_imb', 'bf_imb', 'hl_book_age', 'bf_book_age')
BASE = ROOT/'reports/own-four-directions-20261021T030628'


def value(x):
    return int(x) if isinstance(x, np.integer) else float(x) if math.isfinite(float(x)) else None


def parity(out):
    z = dict(np.load(BASE/'input_BTC.npz'))
    assert np.all(z['dataset'] > 0), 'NOT_OWN_DATA'
    spec = next(c for c in json.loads((BASE/'plan.json').read_text())['candidates'] if c['group'] == 'BTC_LONG')
    expected = signal(features(z), spec['config'])
    path = out/'parity-rows.jsonl'
    with path.open('w') as stream:
        for i in range(len(z['t'])):
            row = {k:value(z[k][i]) for k in FIELDS}
            row['expectedSignal'] = int(expected[i])
            stream.write(json.dumps(row, separators=(',', ':'), allow_nan=False)+'\n')
    subprocess.run(['node', '--no-warnings', str(ROOT/'tools/check_btc_long_parity.cjs'),
                    str(path), str(out/'signal-parity.json'), str(BASE/'BTC_LONG/own_all/baseline-cash.json')], check=True)


def seed(out):
    c = sqlite3.connect('file:'+str(ROOT/'data/unified/market.sqlite3')+'?mode=ro', uri=True)
    c.execute('PRAGMA query_only=ON')
    last = c.execute('SELECT MAX(last_recv_ms) FROM strategy_tape_chunks').fetchone()[0]
    c.close()
    cut = int(min(last, time.time()*1000-2000))//1000*1000
    canonical(out, cut-1800000, cut, 'warmup')
    z = dict(np.load(out/'warmup_BTC.npz'))
    z, counts, manifest = align(z, cut-1800000, cut, 'BTC')
    profile_hash = subprocess.check_output(['node', '-e',
        "process.stdout.write(require('./apps/vchera2/btc-long-candidate.cjs').PROFILE_HASH)"], cwd=ROOT, text=True)
    rows = [{k:value(z[k][i]) for k in FIELDS} for i in range(len(z['t']))]
    result = dict(profileHash=profile_hash, symbol='BTC', scope='OWN_CANONICAL_ONLY',
                  createdMs=int(time.time()*1000), cutMs=cut, rows=rows,
                  source=dict(chunks=len(manifest), bookCoverage=counts))
    save_json(out/'btc-long-seed.json', result)
    save_json(out/'seed-book-chunks.json', manifest)
    save_json(out/'seed-manifest.json', dict(cut_ms=cut, rows=len(rows),
        sha256=hashlib.sha256((out/'btc-long-seed.json').read_bytes()).hexdigest(),
        scope='OWN_CANONICAL_ONLY', profile_hash=profile_hash, created_ms=int(time.time()*1000)))
    print('SEED', json.dumps(json.loads((out/'seed-manifest.json').read_text())), flush=True)


def refresh(out):
    old = json.loads((out/'btc-long-seed.json').read_text())
    c = sqlite3.connect('file:'+str(ROOT/'data/unified/market.sqlite3')+'?mode=ro', uri=True)
    c.execute('PRAGMA query_only=ON')
    cut = int(min(c.execute('SELECT MAX(last_recv_ms) FROM strategy_tape_chunks').fetchone()[0], time.time()*1000-2000))//1000*1000
    c.close()
    canonical(out, old['cutMs']-60000, cut, 'catchup')
    z = dict(np.load(out/'catchup_BTC.npz'))
    z, counts, manifest = align(z, old['cutMs']-60000, cut, 'BTC')
    added = [{k:value(z[k][i]) for k in FIELDS} for i in range(len(z['t'])) if z['t'][i] > old['cutMs']]
    rows = (old['rows']+added)[-1800:]
    assert all(b['t']-a['t']==1000 for a,b in zip(rows,rows[1:])), 'SEED_GAP'
    assert rows[-1]['t']==cut, 'SEED_CUT'
    old.update(rows=rows, cutMs=cut, createdMs=int(time.time()*1000), previousCutMs=old['cutMs'])
    old['source']['catchupChunks'] = len(manifest)
    save_json(out/'btc-long-seed.json', old)
    save_json(out/'catchup-book-chunks.json', manifest)
    save_json(out/'seed-manifest.json', dict(cut_ms=cut, rows=len(rows),
        sha256=hashlib.sha256((out/'btc-long-seed.json').read_bytes()).hexdigest(),
        scope='OWN_CANONICAL_ONLY', profile_hash=old['profileHash'], created_ms=int(time.time()*1000)))
    print('REFRESHED_SEED',cut,'lag_ms',int(time.time()*1000)-cut,flush=True)


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('mode', choices=['parity', 'seed', 'refresh'])
    p.add_argument('--out', type=Path, required=True)
    args = p.parse_args()
    args.out.mkdir(exist_ok=True)
    {'parity':parity, 'seed':seed, 'refresh':refresh}[args.mode](args.out)
