"""Development sensitivity of frozen pass6 candidates to slower queues."""
import argparse
import csv
import json
import subprocess
from pathlib import Path


def run(parent):
    source=parent/'pass6';out=source/'queue-screen';out.mkdir(exist_ok=True)
    candidates=json.loads((source/'qualified.json').read_text())
    risk_profile=json.loads((source/'spec.json').read_text())['risk_profile']
    results=[]
    for margin,lev in ((1001,1000),(1500,668),(2200,455)):
        subset=[c for c in candidates if c['margin']==margin]
        cases=out/f'cases-{margin}.txt';cases.write_text(''.join(f"{c['entry']} {c['risk']}\n" for c in subset))
        by_case={(c['entry'],c['risk']):dict(c,queues={}) for c in subset}
        for label,ed,cd in [('slow',8,12),('tail',10,20)]:
            path=out/f'{label}-{margin}.csv'
            subprocess.run([str(parent/'search'),str(parent/'aligned/market-later_BTC.bin'),
                            str(source/'signals_BTC.bin'),str(path),'cases',str(cases),
                            '814.598','.000476041580087574',risk_profile,str(margin),str(lev),
                            str(ed),str(cd),'6000'],check=True,capture_output=True)
            for row in csv.DictReader(path.open()):
                by_case[(int(row['entry']),int(row['risk']))]['queues'].setdefault(label,{})[row['fold']]=row
        for c in by_case.values():
            checks=[c['queues'][label][fold] for label in ('slow','tail') for fold in ('1','-1')]
            c['all_slower_pass']=all(int(r['n'])>=30 and float(r['wr'])>=.8 and float(r['net'])>0 and int(r['unknown'])==0 for r in checks)
            c['minimum_slower_WR']=min(float(r['wr']) for r in checks)
            c['minimum_slower_net']=min(float(r['net']) for r in checks)
            results.append(c)
        print('SCREEN',margin,'candidates',len(subset),'pass',sum(c['all_slower_pass'] for c in by_case.values()),flush=True)
    results.sort(key=lambda c:(c['all_slower_pass'],c['minimum_slower_WR'],c['minimum_slower_net']),reverse=True)
    (out/'results.json').write_text(json.dumps(results,indent=2)+'\n')
    for c in results[:5]:print({k:c[k] for k in ('entry','risk','margin','leverage','all_slower_pass','minimum_slower_WR','minimum_slower_net')},flush=True)


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--out',type=Path,required=True)
    run(p.parse_args().out)
