"""Freeze inputs for three directional studies, reading the existing tape only."""
import argparse
import hashlib
import json
import sqlite3
import time
from pathlib import Path

import numpy as np

from prepare import ROOT, canonical, save_json
from align_books import align

OLD=ROOT / 'reports' / 'local-input'


def sha(path):return hashlib.sha256(path.read_bytes()).hexdigest()


def run(out):
    out.mkdir(exist_ok=True)
    spec_path=out/'plan.json'
    if not spec_path.exists():
        c=sqlite3.connect('file:'+str(ROOT/'data/unified/market.sqlite3')+'?mode=ro',uri=True)
        c.execute('PRAGMA query_only=ON');last=c.execute('SELECT MAX(last_recv_ms) FROM strategy_tape_chunks').fetchone()[0];c.close()
        cut=int(min(last,time.time()*1000-2000))//1000*1000
        save_json(spec_path,{'created_ms':int(time.time()*1000),'cut_ms':cut,
                  'groups':['BTC_SHORT','ETH_SHORT','ETH_LONG'],
                  'goal':{'bank':2500,'minimum_notional_exclusive':1000000,'WR':.8,'net_positive':True,'training_and_later_n_min':30},
                  'scope':'Offline research only; no orders, subscriptions, restarts, active settings or source mutations.',
                  'price_proxy':'HL, 1second causal observations and sampled extremes, path age<=6s plus stricter sensitivity.',
                  'historical_split':'train17-18Oct; contiguous19Oct onward starts independently with2500. Historical data now development after previous research. Fresh frozen future required for independent confirmation.',
                  'old_inputs':{s:{'path':str(OLD/('aligned/input_BTC.npz' if s=='BTC' else 'input_ETH.npz')),'sha256':sha(OLD/('aligned/input_BTC.npz' if s=='BTC' else 'input_ETH.npz'))} for s in ('BTC','ETH')}})
    spec=json.loads(spec_path.read_text());cut=spec['cut_ms']
    old_end=int(np.load(spec['old_inputs']['BTC']['path'])['t'][-1])
    if not (out/'tail-coverage.json').exists():canonical(out,old_end-60000,cut,'tail')
    for symbol in ('BTC','ETH'):
        target=out/('input_'+symbol+'.npz')
        if target.exists():continue
        item=spec['old_inputs'][symbol];path=Path(item['path'])
        if sha(path)!=item['sha256']:raise ValueError('OLD_INPUT_CHANGED')
        base=dict(np.load(path));tail=dict(np.load(out/('tail_'+symbol+'.npz')));tail['dataset']=np.full(len(tail['t']),2,dtype=np.int8)
        keep=tail['t']>base['t'][-1]
        common=set(base)&set(tail);z={k:np.concatenate([base[k],tail[k][keep]]) for k in common}
        if not np.all(np.diff(z['t'])==1000):raise ValueError('NONCONTIGUOUS_INPUT')
        if symbol=='BTC':start=old_end-60000
        else:start=int(z['t'][z['dataset']==2][0])-1000
        before={k:hashlib.sha256(z[k].tobytes()).hexdigest() for k in ('hl','bf','hl_low','hl_high')}
        z,counts,manifest=align(z,start,cut,symbol)
        assert all(hashlib.sha256(z[k].tobytes()).hexdigest()==h for k,h in before.items()),'ALIGN_CHANGED_PRICES'
        save_json(out/('books-'+symbol+'.json'),{'symbol':symbol,'counts':counts,'price_hashes_unchanged':before,
                  'definition':'notional +/-2bps, up to20 observed levels; native reconstructed, archive fields preserved',
                  'archive_code':'sources/vlad_trade_journal/scanner/'+('btcy2-lag.js:406' if symbol=='BTC' else 'eth-lag.js:452')})
        save_json(out/('book-chunks-'+symbol+'.json'),manifest);np.savez_compressed(target,**z)
        print('PREPARED',symbol,len(z['t']),counts,flush=True)
    params=out/'parameters.json'
    if not params.exists():params.write_bytes((ROOT/'apps/papertrade-shadow/native-parameters.json').read_bytes())
    save_json(out/'prepared.json',{'completed_ms':int(time.time()*1000),'cut_ms':cut,
              'inputs':{s:sha(out/('input_'+s+'.npz')) for s in ('BTC','ETH')}})


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--out',type=Path,required=True);run(p.parse_args().out)
