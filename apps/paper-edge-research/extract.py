"""Read-only, causal one-second research views. Never opens a network connection."""
import argparse
import hashlib
import json
import math
import sqlite3
import time
import zlib
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[2]
DB = ROOT / 'data/unified/market.sqlite3'


def save_json(path, value):
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False, allow_nan=False) + '\n')


class View:
    def __init__(self, start, end):
        self.first = (int(start) // 1000 + 1) * 1000
        self.t = np.arange(self.first, int(end) // 1000 * 1000 + 1, 1000, dtype=np.int64)
        self.q = {v: np.full((len(self.t), 6), np.nan) for v in ('hl', 'bf', 'bs')}
        self.book = {v: np.full((len(self.t), 2), np.nan) for v in self.q}
        self.gaps = np.zeros(len(self.t), dtype=bool)
        self.last_source = {}
        self.counts = {'quotes': 0, 'books': 0, 'regressions': 0, 'bad_clock': 0}

    def cell(self, recv):
        # Even an event exactly on a boundary waits until the next grid step.
        return int(recv) // 1000 + 1 - self.first // 1000

    def quote(self, venue, recv, source, bid, ask, low=None, high=None):
        i = self.cell(recv)
        if not 0 <= i < len(self.t) or not 0 < bid <= ask:
            return
        if source is None or not -200 <= recv - source <= 1500:
            self.counts['bad_clock'] += 1
            return
        if source < self.last_source.get(venue, -math.inf):
            self.counts['regressions'] += 1
            return
        self.last_source[venue] = source
        q = self.q[venue]
        mid = (bid + ask) / 2
        lo, hi = (mid if low is None else low), (mid if high is None else high)
        q[i, 4] = lo if np.isnan(q[i, 4]) else min(lo, q[i, 4])
        q[i, 5] = hi if np.isnan(q[i, 5]) else max(hi, q[i, 5])
        if np.isnan(q[i, 0]) or recv >= q[i, 0]:
            q[i, :4] = [recv, source, bid, ask]
        self.counts['quotes'] += 1

    def put_book(self, venue, recv, source, bids, asks):
        i = self.cell(recv)
        if not 0 <= i < len(self.t) or source is None or not -200 <= recv-source <= 1500:
            return
        b = sum(float(p)*float(s) for p, s in bids[:10])
        a = sum(float(p)*float(s) for p, s in asks[:10])
        if a+b and (np.isnan(self.book[venue][i, 0]) or recv >= self.book[venue][i, 0]):
            self.book[venue][i] = [recv, (b-a)/(b+a)]
        self.counts['books'] += 1

    def finish(self, path):
        result = {'t': self.t, 'gaps': self.gaps}
        for venue, q in self.q.items():
            idx = np.maximum.accumulate(np.where(np.isfinite(q[:, 0]), np.arange(len(q)), -1))
            ok = idx >= 0
            z = q[np.maximum(idx, 0)]
            mid = (z[:, 2]+z[:, 3])/2
            mid[~ok] = np.nan
            result[venue] = mid
            result[venue+'_age'] = np.where(ok, np.maximum(self.t-z[:, 0], self.t-z[:, 1]), np.inf)
            result[venue+'_spread'] = z[:, 3]-z[:, 2]
            # Within-second extremes are never forward-filled.
            result[venue+'_low'] = q[:, 4]
            result[venue+'_high'] = q[:, 5]
            b = self.book[venue]
            ix = np.maximum.accumulate(np.where(np.isfinite(b[:, 0]), np.arange(len(b)), -1))
            z = b[np.maximum(ix, 0)]
            result[venue+'_imb'] = np.where(ix >= 0, z[:, 1], np.nan)
            result[venue+'_book_age'] = np.where(ix >= 0, self.t-z[:, 0], np.inf)
        np.savez_compressed(path, **result)
        valid = (result['hl_age'] <= 1500) & (result['bf_age'] <= 1500)
        return {'file': path.name, 'sha256': hashlib.sha256(path.read_bytes()).hexdigest(),
                'grid_rows': len(self.t), 'valid_hl_bf_seconds': int(valid.sum()),
                'from_ms': int(self.t[0]), 'to_ms': int(self.t[-1]),
                'gap_seconds': int(self.gaps.sum()), **self.counts}


def canonical(out, start, end, name='canonical'):
    c = sqlite3.connect('file:'+str(DB)+'?mode=ro', uri=True)
    c.execute('PRAGMA query_only=ON')
    views = {s: View(start, end) for s in ('BTC', 'ETH')}
    manifest = []; last = {}; previous_boot = None; events = 0
    for cid, boot, sha, blob in c.execute('SELECT id,boot_id,sha256,data_z FROM strategy_tape_chunks WHERE last_recv_ms>=? AND first_recv_ms<=? ORDER BY id', (start, end)):
        raw = zlib.decompress(blob)
        if hashlib.sha256(raw).hexdigest() != sha:
            raise ValueError('TAPE_CHECKSUM')
        manifest.append([cid, boot, sha])
        for e in json.loads(raw):
            recv = e['recv_ms']; seq = e['sequence']
            if seq <= last.get(boot, 0):
                continue
            gap = (boot != previous_boot and previous_boot is not None) or (boot in last and seq != last[boot]+1)
            last[boot] = seq; previous_boot = boot
            if not start <= recv <= end:
                continue
            events += 1
            if gap:
                for v in views.values():
                    i = v.cell(recv)
                    if 0 <= i < len(v.t): v.gaps[i] = True
            if e.get('kind') != 'market' or e.get('symbol') not in views or e.get('venue') not in ('hl','bf','bs'):
                continue
            v = views[e['symbol']]; p = e['payload']; venue = e['venue']
            if p.get('frameKind') == 'quote':
                v.quote(venue, recv, e.get('source_ms'), float(p['bid']), float(p['ask']))
            elif p.get('frameKind') == 'book':
                v.put_book(venue, recv, e.get('source_ms'), p.get('bids',[]), p.get('asks',[]))
        if len(manifest) % 3000 == 0:
            print('canonical chunks', len(manifest), 'events', events, flush=True)
    c.close()
    save_json(out/(name+'-sources.json'), manifest)
    rows = [dict(symbol=s, **v.finish(out/(name+'_'+s+'.npz'))) for s,v in views.items()]
    save_json(out/(name+'-coverage.json'), {'events': events, 'chunks': len(manifest), 'views': rows})
    print(name, rows, flush=True)


def observer(out, end):
    c = sqlite3.connect('file:'+str(DB)+'?mode=ro', uri=True)
    c.row_factory = sqlite3.Row; c.execute('PRAGMA query_only=ON')
    sources = [dict(r) for r in c.execute("SELECT id,path,sha256,cursor,venue_hint,min_ms,max_ms,last_recv_ms FROM sources WHERE role='live' AND dataset='observer' AND venue_hint IN ('hl','binance_futures') ORDER BY id")]
    start = min(r['min_ms'] for r in sources if r['min_ms'])
    views = {s: View(start,end) for s in ('BTC','ETH')}; used=[]
    for source in sources:
        lo = source['id'] << 40; hi = lo+source['cursor']
        first = c.execute('SELECT recv_ms FROM records WHERE id BETWEEN ? AND ? ORDER BY id LIMIT 1',(lo,hi)).fetchone()
        if not first or first[0] > end: continue
        # SQLite's single MAX aggregate selects the other bare columns from its
        # winning row. Exact receipt time is retained; availability is NEXT second.
        query = """SELECT symbol,MAX(recv_ms) AS r,ts_ms,bid,ask,
            MIN((bid+ask)/2) AS low,MAX((bid+ask)/2) AS high
            FROM ticks WHERE record_id BETWEEN ? AND ? AND kind='bbo'
            AND symbol IN ('BTC','ETH') AND bid>0 AND ask>=bid
            AND recv_ms<=? AND recv_ms-ts_ms BETWEEN -200 AND 1500
            GROUP BY symbol,CAST(recv_ms/1000 AS INTEGER)"""
        # Multiple MAX/MIN aggregates make bare-column provenance ambiguous!
        # Use a separate argmax subquery, and join its exact record/sub instead.
        query = """WITH extrema AS (
            SELECT symbol,CAST(recv_ms/1000 AS INTEGER) AS bucket,
            MIN((bid+ask)/2) AS low,MAX((bid+ask)/2) AS high,
            MAX(recv_ms) AS last_recv
            FROM ticks WHERE record_id BETWEEN ? AND ? AND kind='bbo'
            AND symbol IN ('BTC','ETH') AND bid>0 AND ask>=bid
            AND recv_ms<=? AND recv_ms-ts_ms BETWEEN -200 AND 1500
            GROUP BY symbol,bucket), quotes AS (
            SELECT symbol,CAST(recv_ms/1000 AS INTEGER) AS bucket,
            MAX(recv_ms) AS r,ts_ms,bid,ask FROM ticks
            WHERE record_id BETWEEN ? AND ? AND kind='bbo'
            AND symbol IN ('BTC','ETH') AND bid>0 AND ask>=bid
            AND recv_ms<=? AND recv_ms-ts_ms BETWEEN -200 AND 1500
            GROUP BY symbol,bucket)
            SELECT q.*,e.low,e.high FROM quotes q JOIN extrema e USING(symbol,bucket)
            ORDER BY q.r"""
        venue = 'hl' if source['venue_hint']=='hl' else 'bf'
        for r in c.execute(query,(lo,hi,end,lo,hi,end)):
            views[r['symbol']].quote(venue,r['r'],r['ts_ms'],r['bid'],r['ask'],r['low'],r['high'])
        used.append(source)
        if len(used)%80 == 0: print('observer sources',len(used),flush=True)
    c.close();save_json(out/'observer-sources.json',used)
    rows=[dict(symbol=s,**v.finish(out/('observer_'+s+'.npz'))) for s,v in views.items()]
    save_json(out/'observer-coverage.json',rows);print('observer',rows,flush=True)


def archive(out):
    old = ROOT/'reports/net-profit-search-20261020T105714Z/market'
    hashes = {r['file']:r['sha256'] for r in json.loads((old/'input_hashes.json').read_text())}
    results=[]
    for s in ('BTC','ETH'):
        p=old/('archive_'+s+'.npz')
        if hashlib.sha256(p.read_bytes()).hexdigest()!=hashes[p.name]:raise ValueError('ARCHIVE_CACHE_HASH')
        a=np.load(p)['values']; v=View(a[0,0],a[-1,0])
        for t,hl,bf,_ in a:
            if np.isfinite(hl) and np.isfinite(bf):
                v.quote('hl',t,t,hl,hl);v.quote('bf',t,t,bf,bf)
        results.append(dict(symbol=s,source=str(p),source_sha256=hashes[p.name],clock='archive timestamp; receipt/clock not independently verified',**v.finish(out/('archive_'+s+'.npz'))))
    save_json(out/'archive-coverage.json',results);print('archive',results,flush=True)


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('mode',choices=['archive','observer','canonical','fresh']);p.add_argument('--out',type=Path,required=True);a=p.parse_args()
    plan=json.loads((a.out/'plan.json').read_text())
    if a.mode=='archive':archive(a.out)
    elif a.mode=='observer':observer(a.out,1792516271822)
    elif a.mode=='canonical':canonical(a.out,1792516271823,plan['data_cut_ms'])
    else:
        if time.time()*1000 < plan['fresh_to_ms']+3000:raise ValueError('FRESH_WINDOW_NOT_COMPLETE')
        canonical(a.out,plan['fresh_from_ms']-600000,plan['fresh_to_ms'],'fresh')
