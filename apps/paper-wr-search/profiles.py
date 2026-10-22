"""Three frozen, causal BTC research profiles. Never submits actual orders."""
import argparse
import hashlib
import json
import math
import os
import time
from pathlib import Path

import numpy as np

from prepare import persist,ret,rolling_extreme,past_mean,save_json
from prepare_pass2 import past_median


def profile_signals(z):
    h=z['hl'];b=z['bf'];n=len(h)
    valid=np.isfinite(h)&np.isfinite(b)&(z['hl_age']<=1500)&(z['bf_age']<=1500)
    hl=np.where(valid,h,np.nan);bf=np.where(valid,b,np.nan);basis=(bf-hl)/hl*10000
    fast=basis-past_median(basis,120);slow=basis-past_median(basis,1200)
    d=np.sign(np.nan_to_num(fast)).astype(np.int8);cushion=np.minimum(d*fast,d*slow)
    book=(z['hl_imb']+z['bf_imb'])/2
    ready=valid&np.isfinite(past_mean(hl,300,.9))
    base=ready&(cushion>=1.5)&(cushion<=4)&(d*book>=.4)
    # These are the exact same feature definitions as pass2, not indicators
    # recomputed from future position outcomes.
    r5=ret(hl,5);bf5=ret(bf,5)
    range_=(rolling_extreme(hl,300,True)-rolling_extreme(hl,300,False))/hl*10000
    quiet=range_<8
    survive=(np.maximum(0,d*r5)<cushion-.2)&(d*bf5>=0)
    return [persist(np.where(base&mask,d,0).astype(np.int8),3) for mask in (quiet,survive,quiet&survive)]


def write_profiles(out,z):
    all_runs=[]
    for sig in profile_signals(z):
        n=len(sig);starts=np.flatnonzero(np.r_[True,sig[1:]!=sig[:-1]]);ends=np.r_[starts[1:]-1,n-1]
        all_runs.append([(int(i),int(j),int(sig[i])) for i,j in zip(starts,ends) if sig[i]])
    with (out/'signals_BTC.bin').open('wb') as f:
        np.asarray([len(all_runs)],dtype='<i4').tofile(f)
        for runs in all_runs:
            np.asarray([len(runs)],dtype='<i4').tofile(f);np.asarray(runs,dtype='<i4').tofile(f)
    save_json(out/'entries_BTC.json',[{'id':i,'name':name,'runs':len(all_runs[i])} for i,name in enumerate(('QUIET','SURVIVAL','QUIET_AND_SURVIVAL'))])


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--out',type=Path,required=True);a=p.parse_args();parent=a.out;out=parent/'frozen';out.mkdir(exist_ok=True)
    spec={'frozen_ms':int(time.time()*1000),'status':'RESEARCH_CANDIDATES_NOT_LIVE','bank':2500,'margin':1600,'leverage':688,'nominal':1100800,
          'profiles':['QUIET','SURVIVAL','QUIET_AND_SURVIVAL'],'signal':'Three consecutive seconds. d=sign(BF-HL percentage basis minus previous120s upper median); both120s and1200s deviations in d between1.5 and4bps; d*mean(HL,BF top10 notional book imbalances)>=0.4. Archive provided book fields have different/unverified cadence/level semantics; own native data uses10levels.',
          'quiet':'HL preceding300s range<8bps','survival':'max(0,d*HL5s return)<cushion-0.2bps and d*BF5s return>=0','minimum_history_fraction':.9,'quote_age_ms':1500,
          'exit':{'tp_price_bps':.25,'sl_price_bps':6,'maximum_hold_s':120,'minimum_hold_before_tp_s':10,'entry_queue_s':5,'entry_observation_s':1,'close_queue_s':9,'close_observation_s':2,'cooldown_s':30},
          'risk_id_tight':61,'primary_for_fresh':'SURVIVAL chosen for positive training/day19/canonical with WR>=80%; QUIET and combination are separately labeled controls, never pooled.',
          'future_from_ms':int(time.time()*1000)+180000,'future_to_ms':int(time.time()*1000)+1380000,
          'limits':'Retrospective search across many parameters. No exact admission/OI/price-oracle proof. Source gaps and all open/unknown positions remain explicit; archive clocks/book semantics unverified; future sample counts can be small.'}
    save_json(out/'candidate-spec.json',spec)
    z=dict(np.load(parent/'input_BTC.npz'));write_profiles(out,z)
    dest=out/'market_BTC.bin'
    if not dest.exists():os.link(parent/'market_BTC.bin',dest)
    (out/'cases.txt').write_text('0 61\n1 61\n2 61\n')
    print('FROZEN',hashlib.sha256((out/'candidate-spec.json').read_bytes()).hexdigest(),spec['future_from_ms'],spec['future_to_ms'],flush=True)
