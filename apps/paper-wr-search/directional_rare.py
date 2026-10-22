"""Explicitly weaker exploratory screen: small training samples stay labeled."""
import argparse
import csv
import gzip
import json
import subprocess
import time
from pathlib import Path

from prepare import save_json


def run(parent):
    for group in ('ETH_SHORT','ETH_LONG'):
        out=parent/group/'rare';out.mkdir(exist_ok=True)
        save_json(out/'spec.json',{'declared_ms':int(time.time()*1000),'training_min_n':10,'later_min_n':20,'full_min_n':40,
                  'WR':.8,'net_positive':True,'unknown':0,'classification':'EXPLORATORY_SMALL_TRAIN; does not pass primary n30-per-cohort criterion. No independent confirmation.'})
        base=parent/group;params=json.loads((parent/'parameters.json').read_text());row=params['instruments'][1]
        winners=[];counts=[];entries=json.loads((base/'entries.json').read_text())
        for margin,lev in ((1001,1000),(1500,668),(2200,455)):
            folder=out/f'm{margin}-l{lev}';folder.mkdir(exist_ok=True);selected=[]
            with gzip.open(base/f'm{margin}-l{lev}'/'train.csv.gz','rt') as stream:
                for r in csv.DictReader(stream):
                    if int(r['n'])>=10 and float(r['wr'])>=.8 and float(r['net'])>0 and int(r['unknown'])==0:selected.append(r)
            save_json(folder/'selection.json',{'frozen_ms':int(time.time()*1000),'cases':selected})
            cases=folder/'cases.txt';cases.write_text(''.join(r['entry']+' '+r['risk']+'\n' for r in selected));dest=folder/'validation.csv'
            subprocess.run([str(parent/'search'),str(base/'market.bin'),str(base/'signals.bin'),str(dest),'cases',str(cases),str(int(row[13])/1e18),str(int(row[16])/1e18),'grid:'+str(base/'risks.txt'),str(margin),str(lev),'5','18','6000'],check=True,capture_output=True)
            groups={}
            for r in csv.DictReader(dest.open()):groups.setdefault((r['entry'],r['risk']),{})[r['fold']]=r
            qualified=[]
            for (entry,risk),folds in groups.items():
                if all(float(folds[f]['wr'])>=.8 and float(folds[f]['net'])>0 and int(folds[f]['unknown'])==0 and int(folds[f]['n'])>=n for f,n in [('1',20),('-1',40)]):
                    qualified.append({'group':group,'symbol':'ETH','side':-1 if group.endswith('SHORT') else 1,'entry':int(entry),'risk':int(risk),'margin':margin,'leverage':lev,'notional':margin*lev,'config':entries[int(entry)]['config'],'folds':folds,'classification':'EXPLORATORY_SMALL_TRAIN'})
            winners.extend(qualified);counts.append({'margin':margin,'eligible':len(selected),'qualified':len(qualified)})
        save_json(out/'qualified.json',winners);save_json(out/'summary.json',counts);print(group,'RARE',counts,'qualified',len(winners),flush=True)


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--out',type=Path,required=True);run(p.parse_args().out)
