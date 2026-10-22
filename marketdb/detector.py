"""One deterministic detector/markout engine for replay and no-order live shadow."""
import copy
import math
from .evidence import digest
from .latency_gate import TimingGate, validate as validate_gate


def validate(config):
    c=copy.deepcopy(config)
    for k in ['threshold_bps','baseline_tau_ms','max_quote_age_ms','max_source_age_ms','max_source_future_ms','cooldown_ms','warmup_ms','entry_delay_ms','exit_delay_ms','hedge_delay_ms','notional_usd','usdt_per_usdc']:
        if not isinstance(c.get(k),(int,float)) or not math.isfinite(c[k]) or c[k]<0:raise ValueError('Invalid '+k)
    if c['baseline_tau_ms']<=0 or c['max_quote_age_ms']<=0 or c['notional_usd']<=0 or c['usdt_per_usdc']<=0:raise ValueError('Positive scale required')
    if not 0<=c.get('hedge_ratio',0)<=1:raise ValueError('hedge_ratio must be within 0..1')
    if not c.get('symbols') or any(s not in ['BTC','ETH'] for s in c['symbols']):raise ValueError('Unsupported symbols')
    if not c.get('horizons_ms') or any(not isinstance(x,int) or x<=0 for x in c['horizons_ms']):raise ValueError('Positive horizons required')
    for k in ['paper_fee_bps_per_side','hedge_fee_bps_per_side']:
        if c.get(k) is not None and (not isinstance(c[k],(int,float)) or not math.isfinite(c[k]) or c[k]<0):raise ValueError('Invalid fee')
    if 'latency_gate' in c:c['latency_gate']=validate_gate(c['latency_gate'])
    return c


class Detector:
    def __init__(self,config,state=None):
        self.config=validate(config);self.config_hash=digest(self.config)
        self.state=state or {'quotes':{},'baseline':{},'last_signal':{},'started':{},'pending':[],
                             'seen':0,'late':0,'invalid':0,'stale':0,'last_time':0,'signals':0,'outcomes':0,'last_features':{}}
        self.gate=None
        if (self.config.get('latency_gate') or {}).get('enabled',False):
            self.gate=TimingGate(self.config['latency_gate'],self.state.get('latency_gate'))
            self.state['latency_gate']=self.gate.state
            self.state.setdefault('gate_counts',{'allowed':0,'suppressed':0,'reasons':{}})

    def quote(self,symbol,venue,now):
        q=self.state['quotes'].get(symbol+'/'+venue)
        if not q or not 0<=now-(q['recv_ms'] if q['recv_ms'] is not None else q['available_ms'])<=self.config['max_quote_age_ms']:return None
        if q['source_ms'] is not None and not -self.config['max_source_future_ms']<=now-q['source_ms']<=self.config['max_source_age_ms']:return None
        return q

    def _outcomes(self,now):
        out=[];keep=[];cfg=self.config
        for p in self.state['pending']:
            symbol=p['symbol'];h=self.quote(symbol,'hl',now);f=self.quote(symbol,'binance_futures',now)
            if h:
                move=p['sign']*(h['mid']/p['reference_mid']-1)*1e4
                p['adverse_bps']=min(p['adverse_bps'],move);p['favorable_bps']=max(p['favorable_bps'],move)
            if h and h['bid'] is not None and h['ask'] is not None and p.get('entry') is None and now>=p['time_ms']+cfg['entry_delay_ms']:
                p['entry']={'time_ms':now,'price':h['ask'] if p['sign']==1 else h['bid'],'quote':h}
            if f and f['bid'] is not None and f['ask'] is not None and p.get('hedge_entry') is None and now>=p['time_ms']+cfg['hedge_delay_ms']:
                p['hedge_entry']={'time_ms':now,'price':f['bid'] if p['sign']==1 else f['ask'],'quote':f}
            for horizon in cfg['horizons_ms']:
                if horizon in p['done'] or now<p['time_ms']+horizon:continue
                if not h:continue
                markout=p['sign']*(h['mid']/p['reference_mid']-1)*1e4
                details={'reference_mid':p['reference_mid'],'mark_quote':h,'signal_time_ms':p['time_ms'],
                         'entry':p.get('entry'),'hedge_entry':p.get('hedge_entry'),'model':'counterfactual_bbo',
                         'net_pnl_usd':None,'fees_known':False,'funding_modelled':False,'liquidation_modelled':False,
                         'currency_conversion_assumption':cfg['usdt_per_usdc']}
                out.append({'signal_key':p['key'],'horizon_ms':horizon,'observed_ms':now,'markout_bps':markout,
                            'adverse_bps':p['adverse_bps'],'favorable_bps':p['favorable_bps'],'status':'markout','details':details})
                p['done'].append(horizon)
            final_due=p['time_ms']+max(cfg['horizons_ms'])+cfg['exit_delay_ms']
            if now>=final_due and h and h['bid'] is not None and h['ask'] is not None and (f or cfg['hedge_ratio']==0) and p.get('entry'):
                entry=p['entry']['price'];px=h['bid'] if p['sign']==1 else h['ask'];qty=cfg['notional_usd']/entry
                gross=p['sign']*(px-entry)*qty;hedge=0;fees=0;known=cfg.get('paper_fee_bps_per_side') is not None
                if known:fees+=(entry+px)*qty*cfg['paper_fee_bps_per_side']/1e4
                if cfg['hedge_ratio']:
                    if not p.get('hedge_entry'):keep.append(p);continue
                    he=p['hedge_entry']['price'];hx=f['ask'] if p['sign']==1 else f['bid'];hq=qty*cfg['hedge_ratio']
                    hedge=-p['sign']*(hx-he)*hq/cfg['usdt_per_usdc']
                    known=known and cfg.get('hedge_fee_bps_per_side') is not None
                    if cfg.get('hedge_fee_bps_per_side') is not None:fees+=(he+hx)*hq*cfg['hedge_fee_bps_per_side']/1e4/cfg['usdt_per_usdc']
                out.append({'signal_key':p['key'],'horizon_ms':-1,'observed_ms':now,'markout_bps':None,
                            'adverse_bps':p['adverse_bps'],'favorable_bps':p['favorable_bps'],'status':'scenario_closed',
                            'details':{'paper_price_proxy':'HL BBO; actual Paper payout not modelled','paper_entry':p['entry'],
                                       'paper_exit_quote':h,'hedge_entry':p.get('hedge_entry'),'hedge_exit_quote':f,
                                       'paper_linear_gross_usd':gross,'hedge_linear_gross_usd':hedge,'fees_usd':fees if known else None,
                                       'net_pnl_usd':gross+hedge-fees if known else None,'fees_known':known,
                                       'funding_modelled':False,'liquidation_modelled':False}})
            elif now>final_due+60000:
                out.append({'signal_key':p['key'],'horizon_ms':-1,'observed_ms':now,'markout_bps':None,'adverse_bps':p['adverse_bps'],
                            'favorable_bps':p['favorable_bps'],'status':'censored_missing_quotes','details':{}})
            else:keep.append(p)
        self.state['pending']=keep;self.state['outcomes']+=len(out);return out

    def on(self,e):
        s=self.state;cfg=self.config;now=e['time_ms'];s['seen']+=1
        if now<s['last_time']:s['late']+=1;return [],[]
        s['last_time']=now
        if e['kind']=='pressure':
            if self.gate:self.gate.observe(e)
            return [],[]
        symbol=e['symbol'];venue=e['venue']
        if symbol not in cfg['symbols']:return [],[]
        mid_only=e['kind']=='mid' and cfg.get('allow_mid_quotes',False)
        if e['kind']!='bbo' and not mid_only:return [],self._outcomes(now)
        bid=e.get('bid');ask=e.get('ask')
        if mid_only:
            mid=e.get('mid')
            if mid is None or not math.isfinite(mid) or mid<=0:s['invalid']+=1;return [],[]
        elif bid is None or ask is None or not math.isfinite(bid+ask) or bid<=0 or ask<bid:
            s['invalid']+=1;return [],[]
        s['quotes'][symbol+'/'+venue]={'bid':bid,'ask':ask,'mid':e['mid'] if mid_only else (bid+ask)/2,'recv_ms':e['recv_ms'],
                                     'available_ms':now,'source_ms':e.get('source_event_ms') or e.get('ts_ms'),'record_id':e['record_id'],'sub':e['sub'],
                                     'bid_size':e.get('bid_size'),'ask_size':e.get('ask_size')}
        outcomes=self._outcomes(now)
        if mid_only and not e.get('snapshot_final',False):return [],outcomes
        h=self.quote(symbol,'hl',now);f=self.quote(symbol,'binance_futures',now);spot=self.quote(symbol,'binance_spot',now)
        if not h or not f or (cfg['require_spot_confirmation'] and not spot):s['stale']+=1;return [],outcomes
        basis=(f['mid']/h['mid']-1)*1e4
        sb=(spot['mid']/h['mid']-1)*1e4 if spot else None
        old=s['baseline'].get(symbol)
        if old is None:
            s['baseline'][symbol]={'f':basis,'s':sb,'at':now};s['started'][symbol]=now;return [],outcomes
        residual=basis-old['f'];spot_residual=sb-old['s'] if sb is not None and old['s'] is not None else None
        alpha=-math.expm1(-max(0,now-old['at'])/cfg['baseline_tau_ms'])
        old.update(f=old['f']+alpha*(basis-old['f']),s=(old['s']+alpha*(sb-old['s'])) if sb is not None and old['s'] is not None else sb,at=now)
        features={'basis_bps':basis,'residual_bps':residual,'spot_residual_bps':spot_residual,'quotes':{'hl':h,'futures':f,'spot':spot},
                  'baseline_bps':old['f'],'quote_age_ms':{k:now-(q['recv_ms'] if q['recv_ms'] is not None else q['available_ms']) for k,q in [('hl',h),('futures',f),('spot',spot)] if q},
                  'receive_times_available':not mid_only,'bbo_execution_available':not mid_only}
        s['last_features'][symbol]=features
        if now-s['started'][symbol]<cfg['warmup_ms'] or now-s['last_signal'].get(symbol,-1e20)<cfg['cooldown_ms']:return [],outcomes
        if abs(residual)<cfg['threshold_bps'] or (cfg['require_spot_confirmation'] and (spot_residual is None or residual*spot_residual<=0)):return [],outcomes
        sign=1 if residual>0 else -1;side='long' if sign==1 else 'short'
        if self.gate:
            decision=self.gate.evaluate(now);features['latency_gate']=decision
            counts=s['gate_counts'];counts['allowed' if decision['allow'] else 'suppressed']+=1
            for reason in decision['reasons']:counts['reasons'][reason]=counts['reasons'].get(reason,0)+1
        key=digest([self.config_hash,e['record_id'],e['sub'],side])
        signal={'key':key,'time_ms':now,'symbol':symbol,'side':side,'strength_bps':residual,'features':features}
        s['last_signal'][symbol]=now;s['signals']+=1
        s['pending'].append({'key':key,'time_ms':now,'symbol':symbol,'sign':sign,'reference_mid':h['mid'],'done':[],
                             'adverse_bps':0,'favorable_bps':0,'entry':None,'hedge_entry':None})
        return [signal],outcomes
