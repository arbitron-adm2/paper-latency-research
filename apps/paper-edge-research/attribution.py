"""Attribute the frozen real $2k cohort; counterfactuals are conditional, not alpha."""
import argparse
import hashlib
import json
import sqlite3
import statistics
from pathlib import Path

from engine import payment
from extract import DB,save_json


def run(labels_path,out):
    labels=json.loads(labels_path.read_text());params=json.loads((out/'parameters.json').read_text())
    c=sqlite3.connect('file:'+str(DB)+'?mode=ro',uri=True);c.execute('PRAGMA query_only=ON');rows=[]
    for r in labels:
        if r['status']!='closed' or not r.get('position_id'):continue
        if r['margin']!=20 or r['leverage']!=100:raise ValueError('UNEXPECTED_ACTUAL_SIZE')
        evidence=c.execute('SELECT close_evidence FROM bot__btcy2_lag_trades WHERE id=? AND position_id=? AND status=\'closed\'',(r['id'],r['position_id'])).fetchone()
        if not evidence:raise ValueError('ACTUAL_RECORD_NOT_RECONCILED')
        e=json.loads(evidence[0]);side=1 if r['side']=='L' else -1;entry=r['entry_info'];exit_=r['exit_info']
        actual=payment(20,100,r['entry'],r['exit'],side,'BTC',params)
        if abs(actual['net']-(e['realPnl']-e['fee']))>1e-6:raise ValueError('ACTUAL_ACCOUNTING_MISMATCH')
        rows.append({'id':r['id'],'entry_ms':r['opened_at'],'late':bool(entry.get('signalGone')),'direction':side,
                     'net':e['realPnl']-e['fee'],'adjusted':e['realPnl'],'fee':e['fee'],
                     'entry_queue_ms':entry['queueMs'],'entry_slip_dollars':side*(r['entry']-entry['decidedPx']),
                     'exit_slip_dollars':side*(exit_['decidedPx']-r['exit']),
                     'entry_decision_exit_actual':payment(20,100,entry['decidedPx'],r['exit'],side,'BTC',params)['net'],
                     'entry_actual_exit_decision':payment(20,100,r['entry'],exit_['decidedPx'],side,'BTC',params)['net'],
                     'both_decision_prices':payment(20,100,entry['decidedPx'],exit_['decidedPx'],side,'BTC',params)['net']})
    c.close();summary={}
    for name,rr in [('all',rows),('late',[r for r in rows if r['late']]),('signal_not_gone',[r for r in rows if not r['late']]),
                    ('long',[r for r in rows if r['direction']==1]),('short',[r for r in rows if r['direction']==-1])]:
        summary[name]={'n':len(rr),'net':sum(r['net'] for r in rr),'without_best':sum(r['net'] for r in rr)-max([r['net'] for r in rr],default=0),
                       **{k:sum(r[k] for r in rr) for k in ('both_decision_prices','entry_decision_exit_actual','entry_actual_exit_decision')},
                       'mean_entry_adverse_price_dollars':statistics.mean(r['entry_slip_dollars'] for r in rr) if rr else None,
                       'mean_exit_adverse_price_dollars':statistics.mean(r['exit_slip_dollars'] for r in rr) if rr else None}
    result={'labels_source':str(labels_path),'labels_sha256':hashlib.sha256(labels_path.read_bytes()).hexdigest(),
            'scope':'Actual filled/closed $2000 cohort only. Decision-price counterfactuals condition on recorded decisions and exclude rejected/cancelled attempts. They are not strategy replays. Late is known after execution and cannot be a hindsight entry gate.',
            'summary':summary,'trades':rows}
    save_json(out/'actual-trade-attribution.json',result);print(json.dumps(summary,indent=2))


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--labels',type=Path,required=True);p.add_argument('--out',type=Path,required=True);a=p.parse_args();run(a.labels,a.out)
