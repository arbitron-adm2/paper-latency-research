"""Read supplied ETH flow as market data; never execute its supplied code."""
import argparse
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import time

import numpy as np
import orjson

from prepare import ROOT,save_json
from extract import View


def run(parent):
    out=parent/'flow';out.mkdir(exist_ok=True)
    c=sqlite3.connect('file:'+str(ROOT/'data/unified/market.sqlite3')+'?mode=ro',uri=True);c.execute('PRAGMA query_only=ON')
    sources=c.execute("SELECT id,path,sha256,min_ms,max_ms FROM sources WHERE dataset='eth_flow' AND role='flow' ORDER BY min_ms,id").fetchall();c.close()
    start=min(r[3] for r in sources)-1;end=max(r[4] for r in sources)//1000*1000+1000
    split=int(dt.datetime.fromisoformat('2026-10-16T00:00:00+03:00').timestamp()*1000)
    save_json(out/'plan.json',{'declared_ms':int(time.time()*1000),'scope':'New dataset and predictor: supplied ETH flow14-16Oct. Train14-15, later16Oct. Not combined across missing dates with17-21Oct.',
              'split_ms':split,'cut_ms':end,'groups':['ETH_SHORT','ETH_LONG'],'source_schema':'10-potok-efira-0.1s/README.txt: buy/sell are Binance trade volume per step; hb/ha HL BBO; bb/ba Binance futures BBO; trade hi/lo are not HL extremes.',
              'limits':'Snapshot t lacks exchange source-update timestamp. Freshness is snapshot availability, not independently proven exchange freshness; volumes have archive provenance. No raw book data; book filters unavailable. No live changes.'})
    view=View(start,end);n=len(view.t);buy=np.zeros(n);sell=np.zeros(n);count=np.zeros(n);used=[]
    for sid,path,expected,lo,hi in sources:
        digest=hashlib.sha256();rows=0;kinds={}
        with open(path,'rb') as stream:
            for line in stream:
                digest.update(line)
                if not line.strip():continue
                r=orjson.loads(line);rows+=1;t=r['t'];i=view.cell(t)
                if not 0<=i<n:continue
                kinds[r.get('k','')]=kinds.get(r.get('k',''),0)+1
                for venue,a,b in [('hl','hb','ha'),('bf','bb','ba')]:
                    if r.get(a) is not None and r.get(b) is not None:view.quote(venue,t,t,float(r[a]),float(r[b]))
                bv=float(r.get('buy') or 0);sv=float(r.get('sell') or 0);nv=int(r.get('n') or 0)
                if min(bv,sv,nv)<0 or not np.isfinite(bv+sv):raise ValueError('BAD_FLOW')
                buy[i]+=bv;sell[i]+=sv;count[i]+=nv
        if digest.hexdigest()!=expected:raise ValueError('FLOW_SOURCE_HASH')
        used.append({'source_id':sid,'path':path,'sha256':expected,'rows':rows,'kinds':kinds,'min_ms':lo,'max_ms':hi})
        print('FLOW_SOURCE',sid,rows,kinds,flush=True)
    coverage=view.finish(out/'quotes_ETH.npz');z=dict(np.load(out/'quotes_ETH.npz'))
    z.update(flow_buy=buy,flow_sell=sell,flow_n=count,dataset=np.zeros(n,dtype=np.int8));np.savez_compressed(out/'input_ETH.npz',**z)
    save_json(out/'flow-sources.json',used);save_json(out/'flow-coverage.json',coverage)
    for name in ('parameters.json','search','ETH-latency-pool.txt','ETH-latency-archive.json'):
        if not (out/name).exists():os.link(parent/name,out/name)
    save_json(out/'prepared.json',{'completed_ms':int(time.time()*1000),'cut_ms':end,'source_rows':sum(r['rows'] for r in used)})


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--out',type=Path,required=True);run(p.parse_args().out)
