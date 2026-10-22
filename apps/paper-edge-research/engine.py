"""Finite hypothesis grid and conservative delayed execution for research only."""
from collections import Counter
import json
import math

import numpy as np

UNIT = 10**18


def past_mean(x, window, min_fraction=1.):
    """At index i use [i-window,i), excluding current and every future sample."""
    finite = np.isfinite(x)
    prefix = np.r_[0., np.cumsum(np.where(finite, x, 0.))]
    counts = np.r_[0, np.cumsum(finite)]
    result = np.full(len(x), np.nan)
    if len(x) > window:
        sums = prefix[window:-1]-prefix[:-window-1]
        n = counts[window:-1]-counts[:-window-1]
        result[window:] = np.divide(sums,n,out=np.full(len(n),np.nan),where=n>=math.ceil(min_fraction*window))
    return result


def ret(x, seconds):
    out = np.full(len(x), np.nan)
    out[seconds:] = (x[seconds:]/x[:-seconds]-1)*10000
    return out


def persist(signal, seconds=3):
    out = signal.copy()
    for k in range(1,seconds):
        previous = np.zeros(len(signal),dtype=np.int8)
        previous[k:] = signal[:-k]
        out[previous != signal] = 0
    return out


def features(z):
    hl=z['hl'];bf=z['bf']
    valid=(z['hl_age']<=1500)&(z['bf_age']<=1500)&~z['gaps']&np.isfinite(hl)&np.isfinite(bf)
    # A short isolated gap removes its observation, not the preceding five minutes.
    h=np.where(valid,hl,np.nan);b=np.where(valid,bf,np.nan)
    basis=10000*(b-h)/h
    return {'valid':valid,'ready':valid&np.isfinite(past_mean(h,300,.95)),
            'dev':basis-past_mean(basis,120,.95),
            'bf5':ret(b,5),'bf10':ret(b,10),'bf30':ret(b,30),
            'hl10':ret(h,10),'hl_dev':(h/past_mean(h,300,.95)-1)*10000}


def configurations(plan):
    rows=[]
    for family,spec in plan['families'].items():
        thresholds=spec.get('threshold_bps',spec.get('threshold_imbalance'))
        for threshold in thresholds:
            for hold in spec['hold_s']:
                rows.append({'id':family+'-'+str(threshold)+'-'+str(hold),
                             'family':family,'threshold':threshold,'hold':hold})
    return rows


def signals(z,f,cfg):
    threshold=cfg['threshold'];family=cfg['family'];ok=f['ready'].copy()
    if family=='persistent_lag':
        direction=np.sign(np.nan_to_num(f['dev'])).astype(np.int8)
        ok&=(abs(f['dev'])>=threshold)&(direction*f['bf5']>=threshold/2)
    elif family=='momentum':
        direction=np.sign(np.nan_to_num(f['bf30'])).astype(np.int8)
        ok&=(abs(f['bf30'])>=threshold)&(direction*f['hl10']>=.25*abs(f['bf30']))
    elif family=='exhaustion_reversal':
        direction=-np.sign(np.nan_to_num(f['hl_dev'])).astype(np.int8)
        ok&=(abs(f['hl_dev'])>=threshold)&(direction*f['bf5']>=0)&(direction*f['bf10']>=-threshold/2)
    elif family=='book_confirmation':
        imb=(z['hl_imb']+z['bf_imb'])/2
        direction=np.sign(np.nan_to_num(imb)).astype(np.int8)
        ok&=(abs(imb)>=threshold)&(direction*z['hl_imb']>0)&(direction*z['bf_imb']>0)&(direction*f['bf10']>=1)
        ok&=(z['hl_book_age']<=6000)&(z['bf_book_age']<=1500)
    else:raise ValueError(family)
    return persist(np.where(ok,direction,0).astype(np.int8))


def trunc(a,b):
    return (-1 if a<0 else 1)*(abs(a)//b)


def payment(margin,lev,entry,exit_,direction,symbol,params):
    """Integer parity with the existing reconstructed contract accounting."""
    row=params['instruments'][0 if symbol=='BTC' else 1]
    scale=int(row[3]);a=math.floor(entry*scale+1e-7);m=math.floor(exit_*scale+1e-7)
    mar=round(margin*UNIT);threshold=a//50000
    effective=(m-threshold if m>a+threshold else a if m>a else m) if direction==1 else (m+threshold if m+threshold<a else a if m<a else m)
    diff=(effective-a)*direction;g=trunc(mar*lev*diff,a)
    if g<=0:return {'net':max(g,-mar)/UNIT,'adjusted':max(g,-mar)/UNIT,'fee':0.}
    v=abs(effective-a)*UNIT//a;y=v*int(row[12])//UNIT;x=1000000*v*int(row[13])//UNIT
    if not x or not y:return {'net':0.,'adjusted':0.,'fee':0.}
    b=UNIT*UNIT//y;s=int(row[18])*UNIT//x
    factor=(UNIT-int(row[17]))*UNIT//(UNIT+b+s)
    adjusted=g*factor//UNIT;fee=adjusted*int(params['winFeeRaw'])//UNIT
    return {'net':(adjusted-fee)/UNIT,'adjusted':adjusted/UNIT,'fee':fee/UNIT}


def stats(trades):
    values=[r['net'] for r in trades];wins=[v for v in values if v>0];losses=[v for v in values if v<0]
    cash=peak=dd=0.
    for v in values:
        cash+=v;peak=max(peak,cash);dd=max(dd,peak-cash)
    gross_losses=-sum(losses)
    return {'closed':len(values),'wins':len(wins),'losses':len(losses),'zero':sum(v==0 for v in values),
            'wr':len(wins)/len(values) if values else None,
            'net':sum(values),'fees':sum(r.get('fee',0) for r in trades),
            'pf':sum(wins)/gross_losses if gross_losses else None,
            'without_best':sum(values)-max(values,default=0),
            'max_drawdown':dd,'worst':min(values,default=0),'best':max(values,default=0),
            'unknown':sum(r.get('unknown',False) for r in trades),
            'known_closed':sum(not r.get('unknown',False) for r in trades),
            'known_net':sum(r['net'] for r in trades if not r.get('unknown',False)),
            'unknown_full_margin_lower_bound':sum(r['net'] for r in trades if r.get('unknown',False)),
            'late':sum(r.get('late',False) for r in trades),
            'liquidations':sum(r.get('reason')=='liquidation' for r in trades),
            'price_gross':sum(r.get('gross',0) for r in trades),
            'net_after_extra_005_each_action':sum(values)-.1*len(values),
            'half_hour_blocks':len({r['signal_ms']//1800000 for r in trades}),
            'exit_reasons':dict(Counter(r['reason'] for r in trades))}


def replay(z,signal,cfg,params,symbol,start,end,profile='measured',oracle_lag=0):
    """One slot, no cancellation/auto topups; risk also while opens/closes queued.

    For strict unknown paths charge full margin as a lower-bound scenario and
    label them UNKNOWN. This cannot establish profit or real execution there.
    """
    delays={'measured':(5,1,9,2),'stress':(8,2,12,3),'instant':(0,0,0,0)}[profile]
    entry_delay,entry_seen,exit_delay,exit_seen=delays
    t=z['t'];hl=z['hl'];scale=int(params['instruments'][0 if symbol=='BTC' else 1][3]);margin=20.;lev=100
    buffer=int(params['instruments'][0 if symbol=='BTC' else 1][16])/UNIT
    bank=2300.;trades=[];skips=Counter();next_free=0;signal_count=0
    candidates=np.flatnonzero((signal!=0)&(t>=start)&(t<=end-(cfg['hold']+30)*1000))
    for i in candidates:
        if i<next_free:continue
        if bank<margin:skips['capital']+=1;break
        signal_count+=1;direction=int(signal[i]);ei=i+entry_delay;entry_q=ei-oracle_lag
        if ei>=len(t) or entry_q<0:break
        # Oracle lag is a separate stress scenario, never future interpolation.
        entry=float(math.floor(hl[entry_q]*scale+1e-7)/scale) if np.isfinite(hl[entry_q]) else math.nan
        late=signal[ei]!=direction
        r={'signal_ms':int(t[i]),'entry_ms':int(t[ei]),'direction':direction,'entry':entry if math.isfinite(entry) else None,'late':bool(late),'unknown':False}
        def unknown(at,why):
            r.update(exit_ms=int(t[min(at,len(t)-1)]),exit=None,reason=why,net=-margin,fee=0.,gross=0.,unknown=True)
        if not math.isfinite(entry) or z['hl_age'][entry_q]>1500 or z['gaps'][i:ei+1].any():
            unknown(ei,'unknown_entry');j=ei
        else:
            bust=entry*(lev-direction)/(lev*(1-direction*buffer))
            due=None;reason=None;j=ei
            while j<len(t) and t[j]<=end:
                qi=j-oracle_lag
                if z['gaps'][j] or z['hl_age'][qi]>1500 or not np.isfinite(hl[qi]):
                    unknown(j,'unknown_path');break
                # Entry bucket extremes happened BEFORE physical entry.
                worst=z['hl_low'][j] if direction==1 else z['hl_high'][j]
                if j>ei and np.isfinite(worst) and direction*(worst-bust)<=0:
                    r.update(exit_ms=int(t[j]),exit=float(worst),reason='liquidation',net=-margin,fee=0.,gross=margin*lev*direction*(worst-entry)/entry);break
                if due is not None and j>=due:
                    xp=math.floor(hl[qi]*scale+1e-7)/scale
                    r.update(exit_ms=int(t[j]),exit=xp,reason=reason,gross=margin*lev*direction*(xp-entry)/entry,**payment(margin,lev,entry,xp,direction,symbol,params));break
                if j>=ei+entry_seen and due is None:
                    move=direction*(hl[qi]/entry-1)*10000
                    worst_move=direction*(worst/entry-1)*10000 if j>ei and np.isfinite(worst) else move
                    # SL is triggered by sampled extremes; TP requires observed close.
                    if min(move,worst_move)<=-4:reason='sl'
                    elif move>=8:reason='tp'
                    elif j>=ei+cfg['hold']:reason='timeout'
                    if reason:
                        due=j+exit_delay
                        if exit_delay==0:continue
                j+=1
            else:
                unknown(min(j,len(t)-1),'unknown_window_end')
        trades.append(r);bank+=r['net']
        next_free=j+exit_seen+30
    summary=stats(trades)
    summary.update(signals_considered=signal_count,skips=dict(skips),initial_capital=2300.,final_capital=bank,
                   model=profile,oracle_lag_s=oracle_lag,margin=margin,leverage=lev,notional=margin*lev,
                   start_ms=int(start),end_ms=int(end),slot='one per isolated strategy; no topups')
    return summary,trades
