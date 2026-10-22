"""Causal feature matrix and run-length encoded entry hypotheses, offline only."""
import argparse
import hashlib
import importlib.util
import json
import math
import sqlite3
import sys
from pathlib import Path

import numpy as np

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'apps/paper-edge-research'))
from engine import past_mean,ret,persist
from extract import canonical,save_json

OLD=ROOT / 'reports' / 'local-input'


def rolling_extreme(a,w,maximum):
    from collections import deque
    q=deque();out=np.full(len(a),np.nan)
    for i,x in enumerate(a):
        while q and q[0]<i-w:q.popleft()
        if i>=w and q:out[i]=a[q[0]]
        if math.isfinite(x):
            while q and (a[q[-1]]<=x if maximum else a[q[-1]]>=x):q.pop()
            q.append(i)
    return out


def archive_books(symbol,t):
    c=sqlite3.connect('file:'+str(ROOT/'data/unified/market.sqlite3')+'?mode=ro',uri=True)
    rows=c.execute("SELECT id,path,sha256 FROM sources WHERE role='market' AND dataset='market_snapshot' AND format='jsonl' AND symbol_hint=? ORDER BY id",(symbol,)).fetchall();c.close()
    values=np.full((len(t),3),np.nan);last=np.full(len(t),-np.inf);manifest=[]
    for sid,path,sha in rows:
        p=Path(path);p=p if p.is_absolute() else ROOT/p;h=hashlib.sha256();count=0
        with p.open('rb') as f:
            for line in f:
                h.update(line)
                if not line.strip():continue
                r=json.loads(line);count+=1;i=int(r[0])//1000+1-int(t[0])//1000
                if 0<=i<len(t) and r[0]>=last[i] and len(r)>4 and isinstance(r[4],list) and len(r[4])>=3:
                    values[i]=[float(x)/100 if x is not None else np.nan for x in r[4][:3]];last[i]=r[0]
        digest=h.hexdigest()
        if sha and digest!=sha:raise ValueError('ARCHIVE_RAW_HASH')
        manifest.append({'id':sid,'path':str(p),'rows':count,'sha256':digest})
    idx=np.maximum.accumulate(np.where(np.isfinite(last),np.arange(len(t)),-1))
    known=idx>=0;result=values[np.maximum(idx,0)];result[~known]=np.nan
    return result,manifest


def load_all(out,symbol):
    chunks=[];manifest=[]
    for name in ('archive','observer','canonical'):
        path=OLD/(name+'_'+symbol+'.npz');z=dict(np.load(path));n=len(z['t'])
        if name=='archive':
            books,src=archive_books(symbol,z['t']);manifest+=src
            for j,v in enumerate(('hl','bf','bs')):z[v+'_imb']=books[:,j]
        next_start=int(np.load(OLD/('observer_'+symbol+'.npz'))['t'][0]) if name=='archive' else int(np.load(OLD/('canonical_'+symbol+'.npz'))['t'][0]) if name=='observer' else math.inf
        keep=z['t']<next_start
        z={k:v[keep] for k,v in z.items()};z['dataset']=np.full(len(z['t']),('archive','observer','canonical').index(name),dtype=np.int8);chunks.append(z)
        manifest.append({'cache':str(path),'sha256':hashlib.sha256(path.read_bytes()).hexdigest(),'retained':int(keep.sum())})
    path=out/('tail_'+symbol+'.npz');z=dict(np.load(path));keep=z['t']>chunks[-1]['t'][-1];z={k:v[keep] for k,v in z.items()};z['dataset']=np.full(len(z['t']),2,dtype=np.int8);chunks.append(z)
    common=set.intersection(*(set(z) for z in chunks));result={k:np.concatenate([z[k] for z in chunks]) for k in common}
    if not np.all(np.diff(result['t'])==1000):raise ValueError('NONCONTIGUOUS_GRID')
    save_json(out/('sources_'+symbol+'.json'),manifest)
    np.savez_compressed(out/('input_'+symbol+'.npz'),**result)
    return result


def make(out,symbol,z):
    hl=z['hl'];bf=z['bf'];n=len(hl)
    valid=np.isfinite(hl)&np.isfinite(bf)&(z['hl_age']<=1500)&(z['bf_age']<=1500)
    h=np.where(valid,hl,np.nan);b=np.where(valid,bf,np.nan);basis=10000*(b-h)/h
    f={};f['ready']=valid&np.isfinite(past_mean(h,300,.9))
    for w in (5,10,30,60,120,300,600):f['h'+str(w)]=ret(h,w);f['b'+str(w)]=ret(b,w)
    for w in (30,120,300,900,1200):
        f['dev'+str(w)]=basis-past_mean(basis,w,.9)
        mean=past_mean(h,w,.9);f['mean'+str(w)]=(h/mean-1)*10000
        # Center before the squared sum: stable variance at BTC price levels.
        origin=float(h[np.isfinite(h)][0]);x=h-origin
        variance=np.maximum(0,past_mean(x*x,w,.9)-past_mean(x,w,.9)**2)
        f['z'+str(w)]=np.divide(h-mean,np.sqrt(variance),out=np.zeros(n),where=variance>1e-10)
    for w in (60,300):
        hi=rolling_extreme(h,w,True);lo=rolling_extreme(h,w,False)
        f['range'+str(w)]=(hi-lo)/h*10000;f['break'+str(w)]=np.where(h>hi,1,np.where(h<lo,-1,0)).astype(np.int8)
    f['book']=(z['hl_imb']+z['bf_imb'])/2
    f['book3']=(z['hl_imb']+z['bf_imb']+z['bs_imb'])/3
    hour=((z['t']//1000+10800)%86400)/3600
    regimes={'all':np.ones(n,dtype=bool),'quiet':f['range300']<8,'active':f['range300']>=8,
             'NY':(hour>=16.5)&(hour<18),'night':(hour<8)|(hour>=21)}
    entries=[];segments=[]
    def add(family,description,direction,condition,regime='all',persistence=1):
        sig=persist(np.where(f['ready']&condition&regimes[regime],direction,0).astype(np.int8),persistence)
        # Consecutive same-direction eligibility is run-length encoded losslessly.
        change=np.r_[True,sig[1:]!=sig[:-1]];starts=np.flatnonzero(change);ends=np.r_[starts[1:]-1,n-1]
        runs=[(int(i),int(j),int(sig[i])) for i,j in zip(starts,ends) if sig[i]]
        if not runs:return
        idx=len(entries);entries.append({'id':idx,'family':family,'description':description,'regime':regime,'persistence':persistence,'eligible_seconds':int((sig!=0).sum()),'runs':len(runs)})
        segments.append(runs)
    # Pass1 is deliberately price-led: book-rich families remain independently
    # labeled and can be enabled as a separate pass without changing outcomes.
    for w in (30,120,300,1200):
        x=f['dev'+str(w)];direction=np.sign(np.nan_to_num(x)).astype(np.int8)
        for threshold in (.3,.6,1,1.5,2,3):
            for confirmation in ('none','bf','book'):
                cond=abs(x)>=threshold
                if confirmation=='bf':cond&=direction*f['b5']>=0
                if confirmation=='book':cond&=direction*f['book']>=.4
                add('basis',f'BF-HL deviation baseline{w}s >= {threshold}bps; {confirmation}',direction,cond,persistence=3)
    for venue in ('h','b'):
        for w in (5,30,60,120):
            x=f[venue+str(w)];d=np.sign(np.nan_to_num(x)).astype(np.int8)
            for threshold in (1,2,4,8):
                for reverse in (False,True):
                    add('return_reversal' if reverse else 'momentum',f'{venue}{w}s absreturn>={threshold}bps; reverse={reverse}',-d if reverse else d,abs(x)>=threshold,persistence=3)
    for w in (120,300,900):
        x=f['z'+str(w)];d=-np.sign(np.nan_to_num(x)).astype(np.int8)
        for threshold in (1,1.5,2,2.5):
            for confirmation in ('none','stall'):
                cond=abs(x)>=threshold
                if confirmation=='stall':cond&=d*f['b5']>=0
                add('z_reversal',f'HL zscore{w}s>={threshold}; {confirmation}',d,cond,persistence=3)
    for threshold in (4,8,12):
        trend=np.sign(np.nan_to_num(f['h300'])).astype(np.int8)
        for pullback in (.5,1,2):
            add('trend_pullback',f'trend300>={threshold}bps; opposing BF5>={pullback}bps',trend,(abs(f['h300'])>=threshold)&(trend*f['b5']<=-pullback),persistence=3)
    for w in (60,300):
        for reverse in (False,True):
            add('breakout_fade' if reverse else 'breakout',f'HL crosses past{w}s extreme; reverse={reverse}',-f['break'+str(w)] if reverse else f['break'+str(w)],f['break'+str(w)]!=0,persistence=1)
    for threshold in (.4,.6,.8):
        d=np.sign(np.nan_to_num(f['book'])).astype(np.int8)
        add('book',f'HL/BF average book>={threshold}; compatible basis',d,(abs(f['book'])>=threshold)&(d*f['dev120']>=.3),persistence=3)
    # Fixed session/volatility versions of a small declared set, not selected
    # based on their PnL. Entry registry is frozen before screen outcomes.
    for regime in ('quiet','active','NY','night'):
        for typ,w,threshold in [('basis',120,.6),('momentum',30,2),('z_reversal',300,1.5)]:
            x=f['dev'+str(w)] if typ=='basis' else f['b'+str(w)] if typ=='momentum' else f['z'+str(w)]
            d=np.sign(np.nan_to_num(x)).astype(np.int8)*(-1 if typ=='z_reversal' else 1)
            add(typ,f'{typ}{w} threshold{threshold}',d,abs(x)>=threshold,regime,persistence=3)
    for d in (-1,1):
        add('control',f'always direction{d}; one slot',np.full(n,d),np.ones(n,dtype=bool))
    scale=10 if symbol=='BTC' else 100
    day=(z['t']//1000+10800)//86400
    d19=int((np.datetime64('2026-10-19')-np.datetime64('1970-01-01'))/np.timedelta64(1,'D'))
    fold=np.where(day<d19,0,np.where(day==d19,1,np.where(z['dataset']==0,2,np.where(z['dataset']==1,3,4))))
    matrix=np.column_stack([z['t']/1000,np.floor(hl*scale+1e-7),
                            np.floor(z['hl_low']*scale+1e-7),np.ceil(z['hl_high']*scale-1e-7),
                            z['hl_age'],z['bf_age'],z['gaps'],fold]).astype('<f8')
    matrix.tofile(out/('market_'+symbol+'.bin'))
    with (out/('signals_'+symbol+'.bin')).open('wb') as stream:
        np.asarray([len(segments)],dtype='<i4').tofile(stream)
        for runs in segments:
            np.asarray([len(runs)],dtype='<i4').tofile(stream);np.asarray(runs,dtype='<i4').tofile(stream)
    save_json(out/('entries_'+symbol+'.json'),entries)
    save_json(out/('matrix_'+symbol+'.json'),{'rows':n,'columns':['time_s','hl_raw','hl_low_raw','hl_high_raw','hl_age','bf_age','capture_marker','fold'],'scale':scale,'entries':len(entries),'ready_seconds':int(f['ready'].sum()),'source_clock':'archive timestamps / own recv; all inputs causal at next grid boundary','sha256':hashlib.sha256((out/('market_'+symbol+'.bin')).read_bytes()).hexdigest()})
    print('PREPARED',symbol,'rows',n,'entries',len(entries),'ready',int(f['ready'].sum()),flush=True)


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--out',type=Path,required=True);p.add_argument('--tail-only',action='store_true');a=p.parse_args();plan=json.loads((a.out/'plan.json').read_text())
    if not (a.out/'tail_BTC.npz').exists():canonical(a.out,int(np.load(OLD/'canonical_BTC.npz')['t'][-1])-1000,plan['source_cut_ms'],'tail')
    if not a.tail_only:
        for s in ('BTC','ETH'):
            cached=a.out/('input_'+s+'.npz')
            z=dict(np.load(cached)) if cached.exists() else load_all(a.out,s)
            make(a.out,s,z)
