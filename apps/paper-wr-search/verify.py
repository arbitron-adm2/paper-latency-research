"""Exact integer accounting and capital audit of the frozen model candidates."""
import argparse
import csv
import datetime as dt
from decimal import Decimal
import hashlib
import json
import math
from pathlib import Path
import subprocess

from prepare import ROOT,save_json

UNIT=10**18


def trunc(a,b):return (-1 if a<0 else 1)*(abs(a)//b)


def settle(margin,leverage,a,m,side,params,symbol='BTC'):
    mar=int(Decimal(str(margin))*UNIT);threshold=a//50000
    eff=(m-threshold if m>a+threshold else a if m>a else m) if side==1 else (m+threshold if m+threshold<a else a if m<a else m)
    g=trunc(mar*leverage*(eff-a)*side,a)
    if g<=0:return max(g,-mar),0
    row=params['instruments'][0 if symbol=='BTC' else 1];v=abs(eff-a)*UNIT//a;y=v*int(row[12])//UNIT;x=1000000*v*int(row[13])//UNIT
    if not x or not y:return 0,0
    b=UNIT*UNIT//y;s=int(row[18])*UNIT//x
    factor=(UNIT-int(row[17]))*UNIT//(UNIT+b+s);adjusted=g*factor//UNIT;fee=adjusted*int(params['winFeeRaw'])//UNIT
    return adjusted-fee,fee


def wilson(w,n):
    z=1.959963984540054;p=w/n;den=1+z*z/n;center=(p+z*z/(2*n))/den;radius=z*math.sqrt(p*(1-p)/n+z*z/(4*n*n))/den
    return [center-radius,center+radius]


def run(parent,folder='frozen',scopes=('full','later')):
    out=parent/folder;params_path=out/'parameters.json'
    if not params_path.exists():params_path.write_bytes((ROOT/'apps/papertrade-shadow/native-parameters.json').read_bytes())
    params=json.loads(params_path.read_text());spec=json.loads((out/'candidate-spec.json').read_text())
    margin=spec['margin'];lev=spec['leverage'];all_results=[]
    definitions=spec.get('cases',[{'entry':i,'risk':61} for i in range(3)])
    risk_profile=spec.get('risk_profile','tight')
    for definition in definitions:
        profile=definition['entry'];risk=definition['risk']
        cases=out/('case-'+str(profile)+'.txt');cases.write_text(str(profile)+' '+str(risk)+'\n')
        for scope in scopes:
            market=out/('market_BTC.bin' if scope=='full' else 'market-later_BTC.bin');path=out/('ledger-'+str(profile)+'-'+scope+'.csv')
            args=[str(parent/'search'),str(market),str(out/'signals_BTC.bin'),str(path),'ledger',str(cases),'814.598','.000476041580087574',risk_profile,str(margin),str(lev),'5','9','6000','-','20261021',str(-1 if scope=='full' else 1)]
            subprocess.run(args,check=True,capture_output=True)
            with path.open() as f:rows=list(csv.DictReader(f))
            bank=2500*UNIT;peak=bank;dd=0;net_total=fee_total=0;daily={};previous_exit=None;known_errors=[];cash=[]
            for r in rows:
                assert bank>=margin*UNIT,'MARGIN_NOT_FUNDED'
                assert previous_exit is None or int(r['signal'])>=previous_exit+32000,'OVERLAPPING_SLOT'
                assert int(r['signal'])<=int(r['entry'])<=int(r['exit'])
                if r['unknown']=='1' or r['reason']=='liquidation':net,fee=-margin*UNIT,0
                else:
                    net,fee=settle(margin,lev,int(float(r['entry_raw'])),int(float(r['exit_raw'])),int(r['side']),params)
                    error=abs(net/UNIT-float(r['net']));known_errors.append(error)
                    assert error<.00001,'SCREEN_INTEGER_ACCOUNTING_MISMATCH'
                bank+=net;net_total+=net;fee_total+=fee;peak=max(peak,bank);dd=max(dd,peak-bank)
                day=dt.datetime.fromtimestamp(int(r['exit'])/1000,dt.timezone(dt.timedelta(hours=3))).date().isoformat();daily[day]=daily.get(day,0)+net
                previous_exit=int(r['exit']);cash.append({'signal_ms':int(r['signal']),'entry_ms':int(r['entry']),'exit_ms':int(r['exit']),'side':int(r['side']),'entry_raw':int(float(r['entry_raw'])),'exit_raw':int(float(r['exit_raw'])),'net_raw':str(net),'fee_raw':str(fee),'bank_raw':str(bank),'reason':r['reason'],'unknown':r['unknown']=='1'})
            n=len(cash);w=sum(int(r['net_raw'])>0 for r in cash);best=max([int(r['net_raw']) for r in cash],default=0)
            result={'profile':profile,'scope':scope,'closed':n,'wins':w,'losses':sum(int(r['net_raw'])<0 for r in cash),'zero':sum(int(r['net_raw'])==0 for r in cash),'WR':w/n if n else None,'WR_wilson95':wilson(w,n) if n else None,
                    'net':net_total/UNIT,'fee':fee_total/UNIT,'initial_bank':2500,'final_bank':bank/UNIT,'closed_drawdown':dd/UNIT,'without_best':(net_total-best)/UNIT,
                    'daily_net_MSK':{k:v/UNIT for k,v in daily.items()},'without_best_day':(net_total-max(daily.values(),default=0))/UNIT,
                    'unknown_trades':sum(r['unknown'] for r in cash),'unknown_policy':'full-margin scenario loss, not observed provider PnL','liquidations':sum(r['reason']=='liquidation' for r in cash),
                    'accounting_max_error':max(known_errors,default=0),'no_topups':True,'overlap':False,'model_goal_met':bool(n>=30 and w/n>=.8 and net_total>0),'actual_execution_verified':False,
                    'ledger_sha256':hashlib.sha256(path.read_bytes()).hexdigest()}
            save_json(out/('cash-'+str(profile)+'-'+scope+'.json'),cash);all_results.append(result);print(result,flush=True)
    save_json(out/'exact-results.json',all_results)


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--out',type=Path,required=True);p.add_argument('--folder',default='frozen');p.add_argument('--later-only',action='store_true');a=p.parse_args();run(a.out,a.folder,('later',) if a.later_only else ('full','later'))
