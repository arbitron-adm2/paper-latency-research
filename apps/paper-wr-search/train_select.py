"""Freeze a diverse shortlist from training only, before opening validation CSVs."""
import argparse
import collections
import csv
import hashlib
import json
import time
from pathlib import Path


def select(out):
    selection={}
    for symbol in ('BTC','ETH'):
        with (out/('train_'+symbol+'.csv')).open() as f:rows=list(csv.DictReader(f))
        eligible=[r for r in rows if int(r['n'])>=30 and float(r['wr'])>=.8 and float(r['net'])>0 and int(r['unknown'])==0]
        eligible.sort(key=lambda r:(float(r['net'])-.5*float(r['drawdown']),float(r['wr'])),reverse=True)
        per_entry=collections.Counter();chosen=[]
        for r in eligible:
            if per_entry[r['entry']]>=3:continue
            chosen.append(r);per_entry[r['entry']]+=1
            if len(chosen)>=100:break
        selection[symbol]={'all_configs':len(rows),'eligible_total':len(eligible),'selected_ms':int(time.time()*1000),'cases':chosen,
                           'training_sha256':hashlib.sha256((out/('train_'+symbol+'.csv')).read_bytes()).hexdigest()}
        (out/('cases_'+symbol+'.txt')).write_text(''.join(r['entry']+' '+r['risk']+'\n' for r in chosen))
        entries=json.loads((out/('entries_'+symbol+'.json')).read_text())
        print(symbol,'configs',len(rows),'eligible',len(eligible),'selected',len(chosen),flush=True)
        for r in chosen[:5]:print(r['entry'],r['risk'],r['n'],r['wr'],r['net'],entries[int(r['entry'])]['description'],flush=True)
    (out/'selection.json').write_text(json.dumps(selection,indent=2)+'\n')


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--out',type=Path,required=True);a=p.parse_args();select(a.out)
