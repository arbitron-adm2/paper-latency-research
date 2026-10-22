"""Recover the actually recorded ten-level ETH books, with explicit provenance."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import time

import numpy as np
import orjson

from align_books import band_imbalance
from prepare import save_json


def run(parent):
    flow=parent/'flow';out=parent/'early-books';out.mkdir(exist_ok=True)
    z=dict(np.load(flow/'input_ETH.npz'));t=z['t'];n=len(t);first=int(t[0])
    stamps={v:np.full(n,np.nan) for v in ('hl','bf')};imbalances={v:np.full(n,np.nan) for v in stamps};count=0
    sources=json.loads((flow/'flow-sources.json').read_text());used=[]
    for source in sources:
        digest=hashlib.sha256();book_count=0
        with open(source['path'],'rb') as stream:
            for line in stream:
                digest.update(line)
                if b'"k":"b"' not in line and b'"k": "b"' not in line:continue
                r=orjson.loads(line)
                if r.get('k')!='b':continue
                book_count+=1;count+=1;i=int(r['t'])//1000+1-first//1000
                if not 0<=i<n:continue
                for venue in stamps:
                    levels=r.get(venue)
                    if levels and len(levels)==2 and levels[0] and levels[1]:
                        stamps[venue][i]=r['t'];imbalances[venue][i]=band_imbalance(levels[0],levels[1],float(z[venue][i]))
        if digest.hexdigest()!=source['sha256'] or book_count!=source['kinds'].get('b',0):raise ValueError('EARLY_BOOK_COVERAGE_OR_HASH')
        used.append({'source_id':source['source_id'],'sha256':source['sha256'],'book_frames':book_count})
        print('EARLY_BOOKS',source['source_id'],book_count,flush=True)
    coverage={}
    for venue in stamps:
        ix=np.maximum.accumulate(np.where(np.isfinite(stamps[venue]),np.arange(n),-1));known=ix>=0
        age=np.where(known,t-stamps[venue][np.maximum(ix,0)],np.inf)
        imb=imbalances[venue][np.maximum(ix,0)];limit=10000 if venue=='hl' else 3000
        z[venue+'_imb']=np.where(known&(age<=limit),imb,np.nan);z[venue+'_book_age']=age
        coverage[venue]=int(np.isfinite(z[venue+'_imb']).sum())
    np.savez_compressed(out/'input_ETH.npz',**z)
    base=json.loads((flow/'plan.json').read_text());base.update(created_ms=int(time.time()*1000),scope='ETH14-16 supplied quotes and raw10level books. Trade volumes are ALL ZERO and cannot be researched as real flow.',
         dataset_training='14-15Oct on independent earlier archive',dataset_later='16Oct',book_depth_levels=10,
         book_definition='notional within +/-2bps using10 recorded levels only, not equivalent to20-level native and newer archive statistic',sources=used,book_coverage_seconds=coverage)
    save_json(out/'plan.json',base)
    for name in ('parameters.json','search','ETH-latency-pool.txt','ETH-latency-archive.json'):
        if not (out/name).exists():os.link(parent/name,out/name)
    save_json(out/'prepared.json',{'completed_ms':int(time.time()*1000),'book_frames':count,'coverage':coverage})
    save_json(flow/'volume-quality.json',{'status':'BLOCKED_ZERO_TRADE_VOLUME','source_rows':sum(r['rows'] for r in sources),
              'flow_buy_nonzero':int(np.count_nonzero(z['flow_buy'])),'flow_sell_nonzero':int(np.count_nonzero(z['flow_sell'])),
              'trade_count_nonzero':int(np.count_nonzero(z['flow_n'])),'note':'README describes trade volume fields, but all provided values are zero. q rows are quote snapshots; b rows contain raw10level books. Zero signal search is not an economic strategy test.'})


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--out',type=Path,required=True);run(p.parse_args().out)
