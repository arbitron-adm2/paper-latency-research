"""Declared second search pass: median cushion, regime and pre-entry survival."""
import argparse
import bisect
import hashlib
import json
import math
import os
import time
from pathlib import Path

import numpy as np

from prepare import past_mean,persist,ret,rolling_extreme,save_json


def past_median(x,w,fraction=.9):
    result=np.full(len(x),np.nan);ordered=[]
    for i,v in enumerate(x):
        if i>=w and len(ordered)>=math.ceil(w*fraction):result[i]=ordered[len(ordered)//2]
        if math.isfinite(v):bisect.insort(ordered,v)
        if i>=w and math.isfinite(x[i-w]):ordered.pop(bisect.bisect_left(ordered,x[i-w]))
    return result


def make(parent,symbol):
    out=parent/'pass2';out.mkdir(exist_ok=True)
    z=dict(np.load(parent/('input_'+symbol+'.npz')));n=len(z['t']);h=z['hl'];b=z['bf']
    valid=np.isfinite(h)&np.isfinite(b)&(z['hl_age']<=1500)&(z['bf_age']<=1500)
    hl=np.where(valid,h,np.nan);bf=np.where(valid,b,np.nan);basis=(bf-hl)/hl*10000
    med={w:past_median(basis,w) for w in (120,1200,1800)}
    fast=basis-med[120];slow={w:basis-med[w] for w in (1200,1800)}
    r5=ret(hl,5);bf5=ret(bf,5);r300=ret(hl,300)
    hi=rolling_extreme(hl,300,True);lo=rolling_extreme(hl,300,False);range_=(hi-lo)/hl*10000
    book=(z['hl_imb']+z['bf_imb'])/2
    ready=valid&np.isfinite(past_mean(hl,300,.9))
    entries=[];all_runs=[]
    def add(name,d,condition,description):
        sig=persist(np.where(ready&condition,d,0).astype(np.int8),3)
        starts=np.flatnonzero(np.r_[True,sig[1:]!=sig[:-1]]);ends=np.r_[starts[1:]-1,n-1]
        runs=[(int(i),int(j),int(sig[i])) for i,j in zip(starts,ends) if sig[i]]
        if not runs:return
        entries.append({'id':len(entries),'family':name,'description':description,'persistence':3,'eligible_seconds':int(np.count_nonzero(sig)),'runs':len(runs)})
        all_runs.append(runs)
    d=np.sign(np.nan_to_num(fast)).astype(np.int8)
    for w in (1200,1800):
        cushion=np.minimum(d*fast,d*slow[w])
        for threshold in (.3,.6,1,1.5):
            for cap in (1,2,4):
                if cap<=threshold:continue
                for confirmation in (0,.4,.6):
                    base=(cushion>=threshold)&(cushion<=cap)
                    if confirmation:base&=d*book>=confirmation
                    for mode in ('all','quiet','trend','survive'):
                        cond=base.copy()
                        if mode=='quiet':cond&=range_<8
                        elif mode=='trend':cond&=d*r300>=0
                        elif mode=='survive':cond&=(np.maximum(0,d*r5)<cushion-.2)&(d*bf5>=0)
                        add('median_cushion',d,cond,f'min(fast120,slow{w})={threshold}..{cap}bps; book>={confirmation}; {mode}')
    for threshold in (.4,.6,.8):
        direction=np.sign(np.nan_to_num(book)).astype(np.int8)
        for fade in (False,True):
            dd=-direction if fade else direction
            for mode in ('quiet','trend'):
                cond=abs(book)>=threshold
                cond&=(range_<8) if mode=='quiet' else (dd*r300>=0)
                cond&=dd*(basis-past_mean(basis,120,.9))>=.3
                add('book_cushion',dd,cond,f'book>={threshold}; fade={fade}; {mode}; basis compatible')
    for sgn in (-1,1):add('control',np.full(n,sgn),np.ones(n,dtype=bool),f'always {sgn}')
    with (out/('signals_'+symbol+'.bin')).open('wb') as stream:
        np.asarray([len(all_runs)],dtype='<i4').tofile(stream)
        for runs in all_runs:
            np.asarray([len(runs)],dtype='<i4').tofile(stream);np.asarray(runs,dtype='<i4').tofile(stream)
    market=out/('market_'+symbol+'.bin')
    if not market.exists():os.link(parent/market.name,market)
    save_json(out/('entries_'+symbol+'.json'),entries)
    print('PASS2',symbol,'entries',len(entries),'configs',len(entries)*512,flush=True)


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--out',type=Path,required=True);a=p.parse_args();out=a.out/'pass2';out.mkdir(exist_ok=True)
    spec={'created_ms':int(time.time()*1000),'parent_plan':str(a.out/'plan.json'),'purpose':'second declared pass, not optimization on validation PnL','source_cut_ms':json.loads((a.out/'plan.json').read_text())['source_cut_ms'],
          'model':'same bank2500/margin2000/leverage550; min(fast120 median deviation,slow1200/1800 median deviation) in direction; past only',
          'tp_bps':[.25,.5,.75,1,1.5,2,3,4],'sl_bps':[.25,.5,1,1.5,2,3,4,6],'hold_s':[30,60,120,300],'minimum_hold_before_tp_s':[0,10],
          'selection':'train17-18 n>=30,WR>=80%,net>0,unknown0; take best3 per entry and best100 overall, frozen before following validation; archive19 already examined diagnostic, unseen later receiving-clock/future data needed',
          'max_configs':400000,'training_only':'All entry descriptions frozen before training outcome screen. Past returns/cushion used to reject predicted adverse travel greater than the remaining edge.'}
    save_json(out/'spec.json',spec)
    for sym in ('BTC','ETH'):make(a.out,sym)
