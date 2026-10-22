"""Exact cash and timing sensitivity of directional research candidates."""
import argparse
import concurrent.futures
import csv
import hashlib
import json
import shutil
import subprocess
import time
from pathlib import Path

import numpy as np

from prepare import save_json
from verify import settle, UNIT, wilson

BTC_POOL=ROOT / 'reports' / 'local-input'


def audit(parent,group,candidate):
    base=parent/group;out=base/'candidate';out.mkdir(exist_ok=True)
    symbol=candidate['symbol'];params=json.loads((parent/'parameters.json').read_text());row=params['instruments'][0 if symbol=='BTC' else 1]
    margin=candidate['margin'];lev=candidate['leverage'];close=9 if symbol=='BTC' else 18
    cases=out/'cases.txt';cases.write_text(f"{candidate['entry']} {candidate['risk']}\n")
    risk='grid:'+str(base/'risks.txt')
    prefix=[str(parent/'search'),str(base/'market.bin'),str(base/'signals.bin')]
    suffix=[str(int(row[13])/UNIT),str(int(row[16])/UNIT),risk,str(margin),str(lev)]
    def options(extra):
        values=['5',str(close),'6000','-','20261021','-1','0']
        values[:len(extra)]=extra
        if (base/'exit-state.bin').exists():values.append(str(base/'exit-state.bin'))
        return values
    spec=dict(candidate,created_ms=int(time.time()*1000),scope='Historical model after adaptive development, no live activation',
              execution={'entry_s':5,'entry_observation_s':1,'close_s':close,'close_observation_s':2,'cooldown_s':30},
              risk_values=json.loads((base/'spec.json').read_text())['risks'][candidate['risk']],
              actual_execution_verified=False,future_test='NOT_STARTED')
    save_json(out/'candidate-spec.json',spec)
    exact=[]
    z=np.load(parent/('input_'+symbol+'.npz'));timestamps=z['t'];dataset=z['dataset']
    for scope,fold in [('full',-1),('later',1)]:
        ledger=out/('ledger-'+scope+'.csv')
        subprocess.run(prefix+[str(ledger),'ledger',str(cases)]+suffix+options(['5',str(close),'6000','-','20261021',str(fold)]),check=True,capture_output=True)
        rows=list(csv.DictReader(ledger.open()));bank=2500*UNIT;peak=bank;dd=0;previous=None;cash=[];error=0;coverage={}
        for r in rows:
            assert int(r['side'])==candidate['side'],'WRONG_DIRECTION'
            assert bank>=margin*UNIT,'UNFUNDED_MARGIN'
            assert previous is None or int(r['signal'])>=previous+32000,'OVERLAP'
            if r['unknown']=='1' or r['reason']=='liquidation':net,fee=-margin*UNIT,0
            else:
                net,fee=settle(margin,lev,round(float(r['entry_raw'])),round(float(r['exit_raw'])),int(r['side']),params,symbol)
                error=max(error,abs(net/UNIT-float(r['net'])));assert error<.00001,'ACCOUNTING_MISMATCH'
            bank+=net;peak=max(peak,bank);dd=max(dd,peak-bank);previous=int(r['exit'])
            idx=int(np.searchsorted(timestamps,int(r['signal'])));source=['archive','observer','canonical'][int(dataset[idx])]
            coverage[source]=coverage.get(source,0)+1
            cash.append(dict(signal_ms=int(r['signal']),entry_ms=int(r['entry']),exit_ms=int(r['exit']),
                side=int(r['side']),entry_raw=round(float(r['entry_raw'])),exit_raw=round(float(r['exit_raw'])),
                net_raw=str(net),fee_raw=str(fee),bank_raw=str(bank),reason=r['reason'],unknown=r['unknown']=='1',source=source))
        n=len(cash);w=sum(int(r['net_raw'])>0 for r in cash);net=bank-2500*UNIT
        result={'scope':scope,'closed':n,'wins':w,'losses':sum(int(r['net_raw'])<0 for r in cash),
                'zero':sum(int(r['net_raw'])==0 for r in cash),'WR':w/n if n else None,'WR_wilson95':wilson(w,n) if n else None,
                'net':net/UNIT,'fee':sum(int(r['fee_raw']) for r in cash)/UNIT,'initial_bank':2500,'final_bank':bank/UNIT,
                'closed_drawdown':dd/UNIT,'worst':min([int(r['net_raw']) for r in cash],default=0)/UNIT,
                'without_best':(net-max([int(r['net_raw']) for r in cash],default=0))/UNIT,'source_entries':coverage,
                'unknown':sum(r['unknown'] for r in cash),'liquidations':sum(r['reason']=='liquidation' for r in cash),
                'integer_accounting_max_error':error,'no_topups':True,'one_slot':True,'actual_execution_verified':False,
                'ledger_sha256':hashlib.sha256(ledger.read_bytes()).hexdigest()}
        save_json(out/('cash-'+scope+'.json'),cash);exact.append(result)
    save_json(out/'exact-results.json',exact)
    pool=out/'latency-pool.txt'
    shutil.copy2(BTC_POOL/'latency-pool.txt' if symbol=='BTC' else parent/'ETH-latency-pool.txt',pool)
    provenance=BTC_POOL/'latency-provenance.json' if symbol=='BTC' else parent/'ETH-latency-archive.json'
    shutil.copy2(provenance,out/'latency-provenance.json')
    slow=out/'slow-pool.txt';slow.write_text('8 2 12 3\n' if symbol=='BTC' else '10 2 30 3\n')
    jobs=[('strict-path-2s',['5',str(close),'2000']),('slow',['5',str(close),'6000',str(slow),'1'])]
    jobs += [('empirical-'+str(seed),['5',str(close),'6000',str(pool),str(seed)]) for seed in range(2026102100,2026102200)]
    for raw in ([10,20] if symbol=='BTC' else [2,5,10]):jobs.append(('adverse-raw-'+str(raw),['5',str(close),'6000','-','20261021','-1',str(raw)]))
    dest=out/'stress';dest.mkdir(exist_ok=True)
    def one(job):
        name,extra=job;path=dest/(name+'.csv')
        subprocess.run(prefix+[str(path),'cases',str(cases)]+suffix+options(extra),check=True,capture_output=True,timeout=45)
        return [dict(scenario=name,**r) for r in csv.DictReader(path.open()) if r['fold'] in ('1','-1')]
    results=[]
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        for rows in executor.map(one,jobs):results.extend(rows)
    save_json(out/'stress-results.json',results);summary=[]
    for fold in ('1','-1'):
        rows=[r for r in results if r['fold']==fold and r['scenario'].startswith('empirical-')]
        item={'fold':fold,'scenarios':len(rows),'WR80_and_profit':sum(float(r['wr'])>=.8 and float(r['net'])>0 and int(r['n'])>=30 for r in rows)}
        for key in ('wr','net','n','unknown','liquidations','capital_blocked'):
            a=np.array([float(r[key]) for r in rows]);item[key]={'min':float(a.min()),'median':float(np.median(a)),'max':float(a.max())}
        summary.append(item)
    save_json(out/'stress-summary.json',summary)
    save_json(out/'complete.json',{'completed_ms':int(time.time()*1000),'status':'AUDITED_HISTORICAL_MODEL_ONLY'})
    print('AUDITED',group,exact,summary,flush=True)


def run(parent):
    for group in ('BTC_SHORT','ETH_SHORT','ETH_LONG'):
        candidates=json.loads((parent/group/'qualified.json').read_text())
        if not candidates:
            print('NO_QUALIFIED',group,flush=True);continue
        audit(parent,group,candidates[0])


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--out',type=Path,required=True);run(p.parse_args().out)
