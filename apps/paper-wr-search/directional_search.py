"""Bounded independent BTC-short / ETH-short / ETH-long offline grids."""
import argparse
import concurrent.futures
import csv
import gzip
import hashlib
import itertools
import json
import math
import subprocess
import time
from pathlib import Path

import numpy as np

from prepare import ROOT, past_mean, persist, ret, rolling_extreme, save_json
from prepare_pass2 import past_median

SIZES=((1001,1000),(1500,668),(2200,455))
SPLIT=1792357200000  # 19Oct00:00 MSK


def definitions(side):
    configs=[]
    def add(**kw):configs.append(dict(side=side,**kw))
    for orientation, threshold, cap, range_, book, persistence in itertools.product(
            (1,-1),(.3,.75,1.5,3),(3,6,12),(8,16,32,1000000),
            ('none','with4','with8','against4'),(1,3,5)):
        if cap<=threshold:continue
        add(family='median',orientation=orientation,threshold=threshold,cap=cap,
            range_max=range_,book=book,persistence=persistence)
    for window,threshold,lag,book,regime,persistence in itertools.product(
            (5,15,30),(.5,1,2,4),(.3,1),('none','with4','with8'),('quiet','active'),(1,3)):
        add(family='momentum',window=window,threshold=threshold,lag=lag,book=book,
            range_max=16 if regime=='quiet' else 1000000,range_min=0 if regime=='quiet' else 16,persistence=persistence)
    for window,threshold,confirm,book,range_,persistence in itertools.product(
            (120,300,900),(1,1.5,2,3),(False,True),('none','with4','with8','against4'),(8,16,32,1000000),(1,3)):
        add(family='reversal',window=window,threshold=threshold,confirm=confirm,book=book,
            range_max=range_,persistence=persistence)
    for orientation,book_threshold,gate,range_,persistence in itertools.product(
            (1,-1),(.4,.6,.8),('basis','pullback'),(16,32),(1,3)):
        add(family='book',orientation=orientation,threshold=book_threshold,gate=gate,
            range_max=range_,book='none',persistence=persistence)
    for trend,pullback,book in itertools.product((2,4,8,16),(.5,1,2),('none','with4')):
        add(family='pullback',trend=trend,pullback=pullback,book=book,range_max=1000000,persistence=3)
    add(family='control',range_max=1000000,book='none',persistence=3)
    return configs


def features(z):
    valid=np.isfinite(z['hl'])&np.isfinite(z['bf'])&(z['hl_age']<=1500)&(z['bf_age']<=1500)
    h=np.where(valid,z['hl'],np.nan);b=np.where(valid,z['bf'],np.nan)
    basis=(b-h)/h*10000
    f={'ready':valid&np.isfinite(past_mean(h,300,.9)),
       'fast':basis-past_median(basis,120),'slow':basis-past_median(basis,1200),
       'book':(z['hl_imb']+z['bf_imb'])/2,
       'range':(rolling_extreme(h,300,True)-rolling_extreme(h,300,False))/h*10000}
    for w in (5,15,30,300):f['h'+str(w)]=ret(h,w);f['b'+str(w)]=ret(b,w)
    origin=float(h[np.isfinite(h)][0]);x=h-origin
    for w in (120,300,900):
        mean=past_mean(h,w,.9);variance=np.maximum(0,past_mean(x*x,w,.9)-past_mean(x,w,.9)**2)
        f['z'+str(w)]=np.divide(h-mean,np.sqrt(variance),out=np.zeros(len(h)),where=variance>1e-10)
    if 'flow_buy' in z:
        for w in (3,10,30):
            buy=past_mean(z['flow_buy'],w,.9)*w;sell=past_mean(z['flow_sell'],w,.9)*w
            f['volume'+str(w)]=buy+sell
            f['flow'+str(w)]=np.divide(buy-sell,buy+sell,out=np.zeros(len(h)),where=buy+sell>0)
        f['b3']=ret(b,3)
    return f


def flow_definitions(side):
    configs=[]
    for family,w,threshold,volume,range_,p in itertools.product(('flow_follow','flow_fade'),(3,10,30),(.25,.5,.75,.9),(5,25,100),(8,16,32),(1,3)):
        configs.append({'side':side,'family':family,'window':w,'threshold':threshold,'volume':volume,'range_max':range_,'book':'none','persistence':p})
    for w,zscore,threshold,range_,p in itertools.product((120,300,900),(1,2,3),(.25,.5,.75),(8,16,32),(1,3)):
        configs.append({'side':side,'family':'flow_turn','window':w,'threshold':threshold,'zscore':zscore,'range_max':range_,'book':'none','persistence':p})
    return configs


def cross_definitions(side):
    configs=[]
    def add(**kw):configs.append(dict(side=side,**kw))
    for w,threshold,ratio,book,range_,p in itertools.product((15,30,60),(2,4,8),(0,.5),('none','with4','with8'),(16,32),(1,3)):
        add(family='btc_lead',window=w,threshold=threshold,ratio=ratio,book=book,range_max=range_,persistence=p)
    for w,beta,threshold,support,book,range_,p in itertools.product((60,300,900),(.5,1,2),(2,4,8,16),(False,True),('none','with4'),(16,32),(1,3)):
        add(family='relative',window=w,beta=beta,threshold=threshold,support=support,book=book,range_max=range_,persistence=p)
    for w,threshold,support,book,range_,p in itertools.product((120,300,900),(1.5,2,3),(5,30,120),('none','with4','with8'),(8,16,32),(1,3)):
        add(family='cross_reversal',window=w,threshold=threshold,support_window=support,book=book,range_max=range_,persistence=p)
    return configs


def cross_features(z,btc):
    if not np.array_equal(z['t'],btc['t']):raise ValueError('CROSS_ASSET_TIME_MISMATCH')
    f=features(z);valid=np.isfinite(btc['bf'])&np.isfinite(btc['hl'])&(btc['hl_age']<=1500)&(btc['bf_age']<=1500)
    b=np.where(valid,btc['bf'],np.nan);h=np.where(f['ready'],z['hl'],np.nan)
    f['ready']&=valid&np.isfinite(past_mean(b,300,.9))
    for w in (5,15,30,60,120,300,900):
        f['btc'+str(w)]=ret(b,w)
        if 'h'+str(w) not in f:f['h'+str(w)]=ret(h,w)
    return f


def signal(f,c):
    d=c['side'];ok=f['ready']&(f['range']<c['range_max'])&(f['range']>=c.get('range_min',0))
    family=c['family']
    if family=='median':
        cushion=np.minimum(d*c['orientation']*f['fast'],d*c['orientation']*f['slow'])
        ok&=(cushion>=c['threshold'])&(cushion<=c['cap'])
    elif family=='momentum':
        bh=f['b'+str(c['window'])];hh=f['h'+str(c['window'])]
        ok&=(d*bh>=c['threshold'])&(d*(bh-hh)>=c['lag'])
    elif family=='reversal':
        ok&=d*f['z'+str(c['window'])]<=-c['threshold']
        if c['confirm']:ok&=d*f['b5']>=0
    elif family=='book':
        ok&=d*c['orientation']*f['book']>=c['threshold']
        if c['gate']=='basis':ok&=d*f['fast']>=.3
        else:ok&=d*f['h30']<=-1
    elif family=='pullback':ok&=(d*f['h300']>=c['trend'])&(d*f['b5']<=-c['pullback'])
    elif family=='btc_lead':
        x=d*f['btc'+str(c['window'])]
        ok&=(x>=c['threshold'])&(d*f['h'+str(c['window'])]<=c['ratio']*x)
    elif family=='relative':
        ok&=d*(f['h'+str(c['window'])]-c['beta']*f['btc'+str(c['window'])])<=-c['threshold']
        if c['support']:ok&=d*f['btc5']>=0
    elif family=='cross_reversal':
        ok&=(d*f['z'+str(c['window'])]<=-c['threshold'])&(d*f['btc'+str(c['support_window'])]>=0)
    elif family in ('flow_follow','flow_fade'):
        w=str(c['window']);orientation=1 if family=='flow_follow' else -1
        ok&=(d*orientation*f['flow'+w]>=c['threshold'])&(f['volume'+w]>=c['volume'])
        if family=='flow_follow':ok&=(d*f['fast']>=0)&(d*f['b5']>=0)
        else:ok&=(d*f['h30']<=-1)&(d*f['b3']>=0)
    elif family=='flow_turn':
        ok&=(d*f['z'+str(c['window'])]<=-c['zscore'])&(d*f['flow30']<=-c['threshold'])&(d*f['flow3']>=0)&(d*f['b3']>=0)
    elif family!='control':raise ValueError('UNKNOWN_FAMILY')
    book=c['book']
    if book=='with4':ok&=d*f['book']>=.4
    elif book=='with8':ok&=d*f['book']>=.8
    elif book=='against4':ok&=d*f['book']<=-.4
    elif book!='none':raise ValueError('UNKNOWN_BOOK_MODE')
    return persist(np.where(ok,d,0).astype(np.int8),c['persistence'])


def write_signals(out,z,configs,btc=None,feature_values=None):
    f=feature_values if feature_values is not None else features(z) if btc is None else cross_features(z,btc)
    unique=[];seen={};aliases=[];n=len(z['t'])
    for index,c in enumerate(configs):
        sig=signal(f,c);starts=np.flatnonzero(np.r_[True,sig[1:]!=sig[:-1]]);ends=np.r_[starts[1:]-1,n-1]
        runs=np.asarray([(int(i),int(j),int(sig[i])) for i,j in zip(starts,ends) if sig[i]],dtype='<i4').reshape(-1,3)
        key=hashlib.sha256(runs.tobytes()).hexdigest()
        if key not in seen:
            seen[key]=len(unique);unique.append({'id':len(unique),'config':c,'first_grid_index':index,
                'eligible_seconds':int(np.count_nonzero(sig)),'runs':len(runs),'signal_sha256':key,'array':runs})
        aliases.append({'grid_index':index,'entry_id':seen[key]})
    with (out/'signals.bin').open('wb') as stream:
        np.asarray([len(unique)],dtype='<i4').tofile(stream)
        for item in unique:
            a=item.pop('array');np.asarray([len(a)],dtype='<i4').tofile(stream);a.tofile(stream)
    save_json(out/'entries.json',unique);save_json(out/'aliases.json',aliases)
    return unique


def qualify(r):return int(r['n'])>=30 and float(r['wr'])>=.8 and float(r['net'])>0


def run_group(parent,group,mode='standard'):
    symbol,orientation=group.split('_');side=-1 if orientation=='SHORT' else 1
    out=parent/group;out.mkdir(exist_ok=True)
    configs=cross_definitions(side) if mode=='cross' else flow_definitions(side) if mode=='flow' else definitions(side)
    if mode=='state':configs=[c for c in configs if c['family']=='reversal']
    risks=list(itertools.product((.25,.5,1,2,4),(1,2,4,6,10),(30,60,120,300),(0,10,20))) if mode in ('standard','early') else list(itertools.product((.5,1,2,4,8),(1,2,4,8),(60,120,300,600),(0,20,60)))
    if mode=='state':
        risks=[(1,sl,hold,minimum,side*target) for sl,hold,minimum,target in itertools.product((1,2,4,6),(60,120,300,600),(0,10,20,40),(-1,-.5,0,.5))]
    spec_path=out/'spec.json'
    if not spec_path.exists():
        data_plan=json.loads((parent/'plan.json').read_text())
        save_json(spec_path,{'created_ms':int(time.time()*1000),'group':group,'symbol':symbol,'side':side,
            'bank':2500,'mode':mode,'sizes':SIZES,'configs':configs,'risks':risks,'maximum_raw_trials':len(configs)*len(risks)*len(SIZES),
            'training':data_plan.get('dataset_training','14-15Oct ETH flow' if mode=='flow' else '17-18Oct')+'; n30 WR80 netpositive unknown0',
            'validation':data_plan.get('dataset_later','16Oct ETH flow' if mode=='flow' else '19Oct onward')+' starting2500, plus full; same n30/WR80/profit. Historical development, no live claim.',
            'execution':{'entry_s':5,'entry_observation_s':1,'close_s':9 if symbol=='BTC' else 18,'close_observation_s':2,
                         'cooldown_s':30,'one_slot':True,'no_topups':True,'no_assumed_cancellations':True},
            'ETH_latency':'5s/18s median scenario from30 archived ETH live-labeled rows; source not independently receipt verified; local observation1/2s assumed. See ETH-latency-archive.json.',
            'deduplication':'Identical complete RLE signals share a replay; all declared configurations retained in aliases, no outcome used for dedup.'})
        (out/'risks.txt').write_text(''.join(' '.join(map(str,r))+'\n' for r in risks))
    z=dict(np.load(parent/('input_'+symbol+'.npz')))
    if not (out/'signals.bin').exists():
        state_features=features(z) if mode=='state' else None
        if state_features is not None:np.asarray(state_features['z900'],dtype='<f8').tofile(out/'exit-state.bin')
        entries=write_signals(out,z,configs,dict(np.load(parent/'input_BTC.npz')) if mode=='cross' else None,state_features);print(group,'ENTRIES',len(entries),'declared',len(configs),flush=True)
    else:entries=json.loads((out/'entries.json').read_text())
    params=json.loads((parent/'parameters.json').read_text());row=params['instruments'][0 if symbol=='BTC' else 1];scale=int(row[3])
    market=out/'market.bin'
    if not market.exists():
        split=json.loads((parent/'plan.json').read_text()).get('split_ms',SPLIT)
        a=np.column_stack([z['t']/1000,np.floor(z['hl']*scale+1e-7),np.floor(z['hl_low']*scale+1e-7),
                           np.ceil(z['hl_high']*scale-1e-7),z['hl_age'],z['bf_age'],z['gaps'],(z['t']>=split).astype(int)]).astype('<f8');a.tofile(market)
    del z
    position=str(int(row[13])/10**18);buffer=str(int(row[16])/10**18)
    delays=['5','9' if symbol=='BTC' else '18','6000']
    if mode=='state':delays+=['-','20261021','-1','0',str(out/'exit-state.bin')]
    winners=[];progress=[];risk_profile='grid:'+str(out/'risks.txt')
    for margin,lev in SIZES:
        folder=out/f'm{margin}-l{lev}';folder.mkdir(exist_ok=True)
        prefix=[str(parent/'search'),str(market),str(out/'signals.bin')]
        suffix=[position,buffer,risk_profile,str(margin),str(lev)]
        selection_path=folder/'selection.json'
        if not selection_path.exists():
            selected=[];total=0;digest=hashlib.sha256()
            with (folder/'train.log').open('w') as log,gzip.open(folder/'train.csv.gz','wt') as gz:
                p=subprocess.Popen(prefix+['/dev/stdout','train','-']+suffix+delays,stdout=subprocess.PIPE,stderr=log,text=True)
                header=p.stdout.readline();names=next(csv.reader([header]));gz.write(header);digest.update(header.encode())
                for line in p.stdout:
                    total+=1;gz.write(line);digest.update(line.encode());r=dict(zip(names,next(csv.reader([line]))))
                    if qualify(r) and int(r['unknown'])==0:selected.append(r)
                if p.wait()!=0:raise RuntimeError('TRAIN_FAILED_'+group)
            selection={'frozen_ms':int(time.time()*1000),'total_replayed':total,'selected':len(selected),
                       'uncompressed_sha256':digest.hexdigest(),'cases':selected}
            save_json(selection_path,selection)
            (folder/'cases.txt').write_text(''.join(r['entry']+' '+r['risk']+'\n' for r in selected))
        else:selection=json.loads(selection_path.read_text())
        val=folder/'validation.csv'
        if not val.exists():subprocess.run(prefix+[str(val),'cases',str(folder/'cases.txt')]+suffix+delays,check=True,capture_output=True)
        groups={}
        for r in csv.DictReader(val.open()):groups.setdefault((r['entry'],r['risk']),{})[r['fold']]=r
        local=[]
        for (e,risk),folds in groups.items():
            if all(qualify(folds[f]) for f in ('0','1','-1')):
                local.append({'group':group,'symbol':symbol,'side':side,'entry':int(e),'risk':int(risk),'margin':margin,
                              'leverage':lev,'notional':margin*lev,'folds':folds,'config':entries[int(e)]['config']})
        save_json(folder/'qualified.json',local);winners.extend(local)
        count={'group':group,'margin':margin,'leverage':lev,'replayed':selection['total_replayed'],'training_eligible':selection['selected'],
               'qualified':len(local),'no_unknown':sum(all(int(c['folds'][f]['unknown'])==0 for f in ('1','-1')) for c in local)}
        progress.append(count);save_json(out/'progress.json',progress);print(json.dumps(count),flush=True)
    winners.sort(key=lambda c:(-sum(int(c['folds'][f]['unknown']) for f in ('1','-1')),
                 min(float(c['folds'][f]['without_best']) for f in ('1','-1')),
                 float(c['folds']['1']['net'])),reverse=True)
    save_json(out/'qualified.json',winners)
    summary={'group':group,'status':'COMPLETED','declared_entries':len(configs),'distinct_entry_signals':len(entries),
             'replayed_configs':sum(r['replayed'] for r in progress),'qualified':len(winners),
             'best':winners[:10],'completed_ms':int(time.time()*1000),'real_execution_verified':False}
    save_json(out/'summary.json',summary);return summary


def run(parent,mode='standard'):
    deadline=time.time()+900
    while not (parent/'prepared.json').exists():
        if time.time()>deadline:raise RuntimeError('INPUT_PREPARATION_TIMEOUT')
        time.sleep(5)
    summaries=[]
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        groups=('ETH_SHORT','ETH_LONG') if mode in ('cross','flow','early') else ('BTC_SHORT','ETH_SHORT','ETH_LONG')
        for result in pool.map(lambda group:run_group(parent,group,mode),groups):
            summaries.append(result);print('COMPLETED',result['group'],'qualified',result['qualified'],flush=True)
    save_json(parent/'search-summary.json',summaries)


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--out',type=Path,required=True);p.add_argument('--mode',choices=['standard','cross','state','flow','early'],default='standard');a=p.parse_args();run(a.out,a.mode)
