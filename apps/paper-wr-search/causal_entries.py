"""Declarative, causal entry rules reusable unchanged on future observations."""
import argparse
import itertools
import json
import time
from pathlib import Path

import numpy as np

from prepare import past_mean, persist, ret, rolling_extreme, save_json
from prepare_pass2 import past_median


def features(z):
    h, b = z['hl'], z['bf']
    valid = np.isfinite(h) & np.isfinite(b) & (z['hl_age'] <= 1500) & (z['bf_age'] <= 1500)
    h = np.where(valid, h, np.nan)
    b = np.where(valid, b, np.nan)
    basis = (b-h)/h*10000
    fast = basis-past_median(basis, 120)
    slow = basis-past_median(basis, 1200)
    direction = np.sign(np.nan_to_num(fast)).astype(np.int8)
    return {'direction': direction, 'cushion': np.minimum(direction*fast, direction*slow),
            'book': (z['hl_imb']+z['bf_imb'])/2,
            'ready': valid & np.isfinite(past_mean(h, 300, .9)),
            'range': (rolling_extreme(h, 300, True)-rolling_extreme(h, 300, False))/h*10000,
            'r5': ret(h,5), 'bf5':ret(b,5), 'r300':ret(h,300)}


def signal(f, config):
    d, cushion = f['direction'], f['cushion']
    eligible = (f['ready'] & (cushion >= config['minimum_cushion']) &
                (cushion <= config['maximum_cushion']) &
                (d*f['book'] >= config['minimum_book']) &
                (f['range'] < config['maximum_range']))
    side = config.get('side', 0)
    if side:
        eligible &= d == side
    mode = config.get('mode', 'quiet')
    if mode == 'survive':
        eligible &= (np.maximum(0,d*f['r5']) < cushion-.2) & (d*f['bf5'] >= 0)
    elif mode == 'trend':
        eligible &= d*f['r300'] >= 0
    elif mode != 'quiet':
        raise ValueError('UNKNOWN_ENTRY_MODE')
    return persist(np.where(eligible, d, 0).astype(np.int8), config['persistence'])


def write_signals(out, z, configs):
    f = features(z)
    entries = []
    with (out/'signals_BTC.bin').open('wb') as stream:
        np.asarray([len(configs)], dtype='<i4').tofile(stream)
        for idx, config in enumerate(configs):
            sig = signal(f, config)
            starts = np.flatnonzero(np.r_[True, sig[1:] != sig[:-1]])
            ends = np.r_[starts[1:]-1, len(sig)-1]
            runs = [(int(i),int(j),int(sig[i])) for i,j in zip(starts,ends) if sig[i]]
            np.asarray([len(runs)], dtype='<i4').tofile(stream)
            np.asarray(runs, dtype='<i4').tofile(stream)
            entries.append({'id':idx, 'config':config, 'runs':len(runs),
                            'eligible_seconds':int(np.count_nonzero(sig))})
    save_json(out/'entries_BTC.json', entries)
    return entries


def prepare(parent):
    out=parent/'pass6';out.mkdir(exist_ok=True)
    configs=[]
    for minimum, maximum, book, range_, persistence, side in itertools.product(
            (.75,1,1.5,2),(3,4,6),(.2,.4,.6,.8),(4,6,8,10,12,16),(1,3,5),(0,1,-1)):
        configs.append({'minimum_cushion':minimum,'maximum_cushion':maximum,
                        'minimum_book':book,'maximum_range':range_,
                        'persistence':persistence,'side':side,'mode':'quiet'})
    risks=list(itertools.product((.25,.5,1),(2,3,4,6),(60,120,180),(5,10,15,20)))
    (out/'risks.txt').write_text(''.join(' '.join(map(str,r))+'\n' for r in risks))
    spec={'created_ms':int(time.time()*1000),'source_folder':'aligned',
          'signals':'pass6/signals_BTC.bin','risk_profile':'grid:'+str(out/'risks.txt'),
          'sizes':[(1001,1000),(1500,668),(2200,455)],'entries':configs,
          'risk_count':len(risks),'max_configs':len(configs)*len(risks)*3,
          'goal':'same train/later/full WR80 profit n30; all history is development, not independent confirmation',
          'purpose':'New entry-regime grid after two exit/sizing grids failed. Thresholds declared together before outcomes. Both directions and each single-direction rule retained as separate strategies.',
          'minimum_notional':1000000,'bank':2500}
    save_json(out/'spec.json',spec)
    entries=write_signals(out,dict(np.load(parent/'aligned/input_BTC.npz')),configs)
    print('PREPARED',len(entries),'risks',len(risks),'configs',spec['max_configs'],flush=True)


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--out',type=Path,required=True)
    prepare(p.parse_args().out)
