"""Frozen finite search, train-only selection, and separate chronological checks."""
import argparse
import datetime as dt
import hashlib
import json
from pathlib import Path

import numpy as np

from engine import configurations, features, signals, replay
from extract import save_json, ROOT

MSK=dt.timezone(dt.timedelta(hours=3))


def ms(s):return int(dt.datetime.fromisoformat(s).replace(tzinfo=MSK).timestamp()*1000)


def load(out,dataset,symbol):
    z=dict(np.load(out/(dataset+'_'+symbol+'.npz')))
    return z,features(z)


def choose(rows,diagnostic=False):
    eligible=[r for r in rows if r['closed']>=15 and (diagnostic or r['unknown']==0)]
    return max(eligible,key=lambda r:(r['net']-.5*r['max_drawdown'],r['config']['id']),default=None)


def run(out,fresh=False):
    plan=json.loads((out/'plan.json').read_text())
    params_path=ROOT/'apps/papertrade-shadow/native-parameters.json'
    params=json.loads(params_path.read_text());configs=configurations(plan)
    if len(configs)!=24:raise ValueError('FINITE_GRID_CHANGED')
    if fresh:
        selection=json.loads((out/'selection.json').read_text());summaries=[];all_trades=[]
        for symbol in ('BTC','ETH'):
            z,f=load(out,'fresh',symbol)
            ids={r['config']['id'] for r in selection[symbol]['family_leaders']}
            for cfg in configs:
                if cfg['id'] not in ids:continue
                sig=signals(z,f,cfg)
                for profile,lag in [('measured',0),('stress',0),('measured',1)]:
                    s,tr=replay(z,sig,cfg,params,symbol,plan['fresh_from_ms'],plan['fresh_to_ms'],profile,lag)
                    s.update(symbol=symbol,dataset='fresh',fold='fresh',config=cfg)
                    summaries.append(s);all_trades.extend(dict(symbol=symbol,config=cfg['id'],model=profile,oracle_lag_s=lag,**r) for r in tr)
        save_json(out/'fresh-results.json',summaries);save_json(out/'fresh-trades.json',all_trades)
        print('fresh',[(r['symbol'],r['config']['id'],r['model'],r['oracle_lag_s'],r['closed'],round(r['net'],5)) for r in summaries],flush=True)
        return
    # Snapshot accounting inputs and research implementation for reproducibility.
    (out/'parameters.json').write_bytes(params_path.read_bytes())
    code=[]
    for p in sorted(Path(__file__).parent.glob('*.py')):
        code.append({'file':str(p.relative_to(ROOT)),'sha256':hashlib.sha256(p.read_bytes()).hexdigest()})
    save_json(out/'code-manifest.json',code)
    selection={};rows=[];trade_rows=[];cached={}
    # No validation dataset is used by the selection function.
    for symbol in ('BTC','ETH'):
        z,f=load(out,'canonical',symbol);cached[symbol]=(z,f);train=[]
        for cfg in configs:
            sig=signals(z,f,cfg)
            s,tr=replay(z,sig,cfg,params,symbol,int(z['t'][0]),ms('2026-10-20T23:30:00'))
            s.update(symbol=symbol,dataset='canonical',fold='train',config=cfg)
            train.append(s);rows.append(s)
        price=choose([r for r in train if r['config']['family']!='book_confirmation'])
        families=[choose([r for r in train if r['config']['family']==family],diagnostic=True) for family in plan['families']]
        selection[symbol]={'winner':price if price and price['net']>0 else None,
                           'best_price_train':price,'family_leaders':[r for r in families if r],
                           'selection_data_end_ms':ms('2026-10-20T23:30:00'),
                           'positive_winner_required':True}
    save_json(out/'selection.json',selection)
    print('SELECTION',json.dumps({s:{'winner':r['winner']['config']['id'] if r['winner'] else None,'leaders':[(v['config']['id'],v['closed'],round(v['net'],5)) for v in r['family_leaders']]} for s,r in selection.items()}),flush=True)
    for symbol in ('BTC','ETH'):
        leader_ids={r['config']['id'] for r in selection[symbol]['family_leaders']}
        for dataset in ('archive','observer','canonical'):
            z,f=cached[symbol] if dataset=='canonical' else load(out,dataset,symbol)
            if dataset=='archive':
                folds=[('archive_'+str(day),ms('2026-10-'+str(day)+'T00:00:00'),min(ms('2026-10-'+str(day)+'T23:59:59'),int(z['t'][-1]))) for day in (17,18,19,20)]
            elif dataset=='observer':
                folds=[('observer_train',ms('2026-10-20T11:00:00'),ms('2026-10-20T15:00:00')),
                       ('observer_validation_NY',ms('2026-10-20T15:00:00'),int(z['t'][-1]))]
            else:folds=[('canonical_validation',ms('2026-10-20T23:30:00'),plan['data_cut_ms'])]
            for cfg in configs:
                if dataset!='canonical' and cfg['family']=='book_confirmation':continue
                sig=signals(z,f,cfg)
                for fold,start,end in folds:
                    s,tr=replay(z,sig,cfg,params,symbol,start,end)
                    s.update(symbol=symbol,dataset=dataset,fold=fold,config=cfg)
                    rows.append(s)
                    if cfg['id'] in leader_ids:
                        trade_rows.extend(dict(symbol=symbol,dataset=dataset,fold=fold,config=cfg['id'],model='measured',oracle_lag_s=0,**r) for r in tr)
                        if fold in ('canonical_validation','observer_validation_NY'):
                            for profile,lag in [('stress',0),('measured',1),('instant',0)]:
                                ss,tt=replay(z,sig,cfg,params,symbol,start,end,profile,lag)
                                ss.update(symbol=symbol,dataset=dataset,fold=fold,config=cfg);rows.append(ss)
                                trade_rows.extend(dict(symbol=symbol,dataset=dataset,fold=fold,config=cfg['id'],model=profile,oracle_lag_s=lag,**r) for r in tt)
            print('DONE',dataset,symbol,flush=True)
    save_json(out/'results.json',rows);save_json(out/'leader-trades.json',trade_rows)
    # Diagnostic maxima are explicitly NOT untouched candidates or deployments.
    for symbol in ('BTC','ETH'):
        for fold in ('canonical_validation','observer_validation_NY'):
            rr=[r for r in rows if r['symbol']==symbol and r['fold']==fold and r['model']=='measured' and r['oracle_lag_s']==0]
            print('VALIDATION',symbol,fold,[(r['config']['id'],r['closed'],round(r['net'],5),r['unknown']) for r in sorted(rr,key=lambda x:x['net'],reverse=True)[:4]],flush=True)


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--out',type=Path,required=True);p.add_argument('--fresh',action='store_true');a=p.parse_args();run(a.out,a.fresh)
