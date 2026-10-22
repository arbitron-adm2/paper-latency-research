"""Align native depth with the archived +/-0.02% notional imbalance definition.

Source scanner was read statically; no supplied archive code is executed.
Quotes and trade prices are not changed. Original research inputs remain intact.
"""
import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import sqlite3
import time
import zlib

import numpy as np

from prepare import ROOT,save_json
from profiles import write_profiles


def band_imbalance(bids,asks,mid):
    if not math.isfinite(mid) or not bids or not asks:return math.nan
    b=sum(float(p)*float(q) for p,q in bids[:20] if float(p)>=mid*(1-.0002))
    a=sum(float(p)*float(q) for p,q in asks[:20] if float(p)<=mid*(1+.0002))
    return math.floor((b-a)/(b+a)*100+.5)/100 if b+a else 0.


def align(z,start,end,symbol='BTC'):
    if symbol not in ('BTC','ETH'):raise ValueError('UNKNOWN_SYMBOL')
    selected=np.flatnonzero((z['t']>=start)&(z['t']<=end));t=z['t'][selected];first=int(t[0]);n=len(t)
    books={v:np.full((n,20,2,2),np.nan) for v in ('hl','bf')}
    stamps={v:np.full((n,2),np.nan) for v in books};last_source={};last_seq={};counts={'book_frames':0,'chunks':0,'unknown_clock':0,'regressions':0}
    c=sqlite3.connect('file:'+str(ROOT/'data/unified/market.sqlite3')+'?mode=ro',uri=True);c.execute('PRAGMA query_only=ON');manifest=[]
    for cid,boot,sha,blob in c.execute('SELECT id,boot_id,sha256,data_z FROM strategy_tape_chunks WHERE last_recv_ms>=? AND first_recv_ms<=? ORDER BY id',(start,end)):
        raw=zlib.decompress(blob)
        if hashlib.sha256(raw).hexdigest()!=sha:raise ValueError('TAPE_HASH')
        manifest.append([cid,boot,sha]);counts['chunks']+=1
        for e in json.loads(raw):
            seq=e['sequence']
            if seq<=last_seq.get(boot,0):continue
            last_seq[boot]=seq
            if not start<=e['recv_ms']<=end or e.get('symbol')!=symbol or e.get('venue') not in books or e.get('payload',{}).get('frameKind')!='book':continue
            v=e['venue'];recv=e['recv_ms'];source=e.get('source_ms')
            if source is None or not -200<=recv-source<=1500:counts['unknown_clock']+=1;continue
            if source<last_source.get(v,-math.inf):counts['regressions']+=1;continue
            last_source[v]=source;i=int(recv)//1000+1-first//1000
            if not 0<=i<n:continue
            if np.isfinite(stamps[v][i,0]) and recv<stamps[v][i,0]:continue
            p=e['payload'];books[v][i]=np.nan
            for side,key in enumerate(('bids','asks')):
                levels=p.get(key,[])[:20]
                for j,(px,qty) in enumerate(levels):books[v][i,j,side]=[float(px),float(qty)]
            stamps[v][i]=[recv,source];counts['book_frames']+=1
        if counts['chunks']%4000==0:print('BOOKS',counts,flush=True)
    c.close()
    for venue in books:
        stamp=stamps[venue];idx=np.maximum.accumulate(np.where(np.isfinite(stamp[:,0]),np.arange(n),-1));age=np.full(n,np.inf);imb=np.full(n,np.nan)
        for i in range(n):
            if idx[i]<0:continue
            k=idx[i];age[i]=max(t[i]-stamp[k,0],t[i]-stamp[k,1]);limit=10000 if venue=='hl' else 3000
            if age[i]>limit:continue
            levels=books[venue][k];b=levels[:,0];a=levels[:,1]
            bids=b[np.isfinite(b).all(axis=1)].tolist();asks=a[np.isfinite(a).all(axis=1)].tolist()
            imb[i]=band_imbalance(bids,asks,float(z[venue][selected[i]]))
        z[venue+'_imb'][selected]=imb;z[venue+'_book_age'][selected]=age
        counts[venue+'_available_seconds']=int(np.isfinite(imb).sum())
    return z,counts,manifest


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--out',type=Path,required=True);a=p.parse_args();parent=a.out;out=parent/'aligned';out.mkdir(exist_ok=True)
    z=dict(np.load(parent/'input_BTC.npz'));native=z['dataset']==2;start=int(z['t'][native][0])-1000;end=int(z['t'][-1]);prices_before={k:hashlib.sha256(z[k].tobytes()).hexdigest() for k in ('hl','bf','hl_low','hl_high')}
    z,counts,manifest=align(z,start,end)
    for k,sha in prices_before.items():assert hashlib.sha256(z[k].tobytes()).hexdigest()==sha,'PRICE_CHANGED'
    np.savez_compressed(out/'input_BTC.npz',**z);save_json(out/'book-alignment.json',{'definition':'notional inside +/-0.0002 of causally available BBO; up to20 observed levels; JS rounding to integer percent; HL book age<=10s BF<=3s','static_source':'sources/vlad_trade_journal/scanner/btcy2-lag.js:406','counts':counts,'quote_hashes_unchanged':prices_before,'source_clock':'receive-time own data; archived book fields preserved'});save_json(out/'source-chunks.json',manifest)
    write_profiles(out,z);dest=out/'market_BTC.bin'
    if not dest.exists():os.link(parent/'market_BTC.bin',dest)
    dest=out/'market-later_BTC.bin'
    if not dest.exists():os.link(parent/'frozen/market-later_BTC.bin',dest)
    spec=json.loads((parent/'frozen/candidate-spec.json').read_text());now=int(time.time()*1000);spec.update(parent_spec_sha256=hashlib.sha256((parent/'frozen/candidate-spec.json').read_bytes()).hexdigest(),semantic_alignment_frozen_ms=now,book_metric='notional within +/-2bps; up to20 levels; JS integer-percent rounding; archived provided values / canonical raw depth',future_from_ms=now+60000,future_to_ms=now+660000,primary_for_fresh='QUIET (profile0), chosen on entire and contiguous later historical replay before reading the new window. Other profiles separate controls. No threshold tuning after this freeze.')
    save_json(out/'candidate-spec.json',spec);(out/'cases.txt').write_text('0 61\n1 61\n2 61\n')
    print('ALIGNED_FROZEN',counts,spec['future_from_ms'],spec['future_to_ms'],flush=True)
