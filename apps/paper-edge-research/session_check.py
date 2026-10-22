"""Retrospective NY cash-open replication, using already frozen family leaders.

Never selects a new parameter combination and never submits orders.
"""
import argparse
import datetime as dt
import json
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np

from engine import features,signals,replay
from extract import save_json


def run(out):
    selection=json.loads((out/'selection.json').read_text());params=json.loads((out/'parameters.json').read_text())
    rows=[];trades=[];ny=ZoneInfo('America/New_York');calendar=[]
    for day in (17,18,19,20):
        open_=dt.datetime(2026,10,day,9,30,tzinfo=ny)
        calendar.append({'day':str(open_.date()),'weekday':open_.strftime('%A'),'cash_weekday':open_.weekday()<5,
                         'open_ms':int(open_.timestamp()*1000)})
        if open_.weekday()>=5:continue
        start=int(open_.timestamp()*1000);end=start+3600000
        dataset='archive' if day==19 else 'observer'
        for symbol in ('BTC','ETH'):
            z=dict(np.load(out/(dataset+'_'+symbol+'.npz')));f=features(z)
            for lead in selection[symbol]['family_leaders']:
                cfg=lead['config']
                if cfg['family']=='book_confirmation':continue
                sig=signals(z,f,cfg);sig[(z['t']<start)|(z['t']>=end)]=0
                for profile in ('measured','stress','instant'):
                    s,rr=replay(z,sig,cfg,params,symbol,start,end+(cfg['hold']+30)*1000,profile)
                    s.update(day=day,dataset=dataset,symbol=symbol,config=cfg,fold='NY_0930_1030')
                    rows.append(s);trades.extend(dict(day=day,symbol=symbol,config=cfg['id'],model=profile,**r) for r in rr)
    save_json(out/'ny-calendar.json',calendar);save_json(out/'ny-session-results.json',rows);save_json(out/'ny-session-trades.json',trades)
    print([(r['day'],r['symbol'],r['config']['id'],r['model'],r['closed'],r['unknown'],round(r['known_net'],5)) for r in rows if r['model']!='instant'],flush=True)


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--out',type=Path,required=True);a=p.parse_args();run(a.out)
