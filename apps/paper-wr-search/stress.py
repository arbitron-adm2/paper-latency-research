"""Fixed-candidate queue sensitivity; scenarios are not independent observations."""
import argparse
import concurrent.futures
import csv
import json
import subprocess
from pathlib import Path

import numpy as np


def run(parent,folder='frozen'):
    out=parent/folder;dest=out/'stress';dest.mkdir(exist_ok=True)
    spec=json.loads((out/'candidate-spec.json').read_text())
    base=[str(parent/'search'),str(out/'market-later_BTC.bin'),str(out/'signals_BTC.bin')]
    args=['cases',str(out/'cases.txt'),'814.598','.000476041580087574',spec.get('risk_profile','tight'),str(spec['margin']),str(spec['leverage'])]
    jobs=[('fixed-8-12',['8','12','6000']),('fixed-10-20',['10','20','6000']),('strict-path-1p5s',['5','9','1500'])]
    jobs += [('empirical-'+str(seed),['5','9','6000',str(out/'latency-pool.txt'),str(seed)]) for seed in range(2026102100,2026102200)]
    def one(job):
        name,extra=job;p=dest/(name+'.csv')
        subprocess.run(base+[str(p)]+args+extra,check=True,capture_output=True,timeout=30)
        with p.open() as f:return name,list(csv.DictReader(f))
    results=[]
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        for name,rows in pool.map(one,jobs):
            results.extend(dict(scenario=name,**r) for r in rows)
    (out/'stress-results.json').write_text(json.dumps(results,indent=2)+'\n')
    summary=[]
    for e in [r['entry'] for r in spec.get('cases',[{'entry':i,'risk':61} for i in range(3)])]:
        for fold in ('1','-1'):
            rr=[r for r in results if r['entry']==str(e) and r['fold']==fold and r['scenario'].startswith('empirical-')]
            row={'entry':e,'fold':fold,'scenarios':len(rr)}
            for key in ('wr','net','n','drawdown','unknown','liquidations','expired'):
                v=np.array([float(r[key]) for r in rr]);row[key]={'min':float(v.min()),'p10':float(np.quantile(v,.1)),'p50':float(np.median(v)),'p90':float(np.quantile(v,.9)),'max':float(v.max())}
            row['joint_WR80_net_positive']=sum(float(r['wr'])>=.8 and float(r['net'])>0 for r in rr)
            summary.append(row)
    (out/'stress-summary.json').write_text(json.dumps(summary,indent=2)+'\n')
    for r in summary:print(r,flush=True)


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--out',type=Path,required=True);p.add_argument('--folder',default='frozen');a=p.parse_args();run(a.out,a.folder)
