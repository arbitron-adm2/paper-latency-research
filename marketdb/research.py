"""Reproducible offline replay and live no-order detection on one SQL timeline."""
import argparse
import datetime as dt
import fcntl
import hashlib
import json
import signal
import time
import uuid
from pathlib import Path
from .core import DEFAULT_ROOT, connect
from .research_schema import init
from .evidence import canonical, digest
from .detector import Detector, validate
from .timeline import events, available
from .lifecycle import read_window, finite


CODE_FILES=['core.py','wire.py','streams.py','timeline.py','detector.py','latency_gate.py','lifecycle.py','evidence.py','research_schema.py','research.py']
def code_hash():return hashlib.sha256(b''.join((Path(__file__).parent/x).read_bytes() for x in CODE_FILES)).hexdigest()


def ms(value,default):
    if value is None:return default
    if value[-1:] in ['s','m','h']:
        return time.time()*1000-float(value[:-1])*{'s':1000,'m':60000,'h':3600000}[value[-1]]
    try:return float(value)
    except ValueError:
        t=dt.datetime.fromisoformat(value.replace('Z','+00:00'))
        if t.tzinfo is None:raise ValueError('Explicit timezone required')
        return t.timestamp()*1000


def save(c,run_id,signals,outcomes,observations=False):
    c.executemany('INSERT OR IGNORE INTO detector_signals(run_id,event_key,recv_ms,symbol,side,strength_bps,features_json) VALUES(?,?,?,?,?,?,?)',
                  [(run_id,s['key'],s['time_ms'],s['symbol'],s['side'],s['strength_bps'],canonical(s['features'])) for s in signals])
    c.executemany('INSERT OR REPLACE INTO detector_outcomes VALUES(?,?,?,?,?,?,?,?,?)',
                  [(run_id,o['signal_key'],o['horizon_ms'],o['observed_ms'],o['markout_bps'],o['adverse_bps'],o['favorable_bps'],o['status'],canonical(o['details'])) for o in outcomes])
    if observations and signals:
        from .evidence import save_event
        for s in signals:
            now=time.time()*1000
            save_event(c,{'data':{'origin':'research_detector','kind':'signal_observed','key':digest([run_id,s['key'],'observed']),
                'event_ms':s['time_ms'],'mode':'shadow','precision':'local_wall_and_monotonic',
                'payload':{'run_id':run_id,'signal_key':s['key'],'detected_clock_ms':s['time_ms'],
                           'observed_wall_ms':now,'symbol':s['symbol'],'side':s['side']}}},now,time.monotonic_ns())


def summary(c,run_id,engine,clock,split=None):
    signals=c.execute('SELECT COUNT(*) FROM detector_signals WHERE run_id=?',(run_id,)).fetchone()[0]
    result={'run_id':run_id,'clock':clock,'events_processed':engine.state['seen'],'late_events_rejected':engine.state['late'],
            'invalid_quotes':engine.state['invalid'],'stale_evaluations_skipped':engine.state['stale'],'signals':signals,
            'pending_scenarios':len(engine.state['pending']),'last_event_ms':engine.state['last_time'],
            'last_features':engine.state['last_features'],'config_sha256':engine.config_hash,
            'code_sha256':c.execute('SELECT code_sha256 FROM detector_runs WHERE id=?',(run_id,)).fetchone()[0],
            'outcomes':[dict(x) for x in c.execute('SELECT horizon_ms,status,COUNT(*) n,AVG(markout_bps) mean_markout_bps FROM detector_outcomes WHERE run_id=? GROUP BY horizon_ms,status',(run_id,))],
            'model_scope':'Counterfactual BBO markouts and delayed linear hedge scenario; no orders. Paper payout/funding/liquidation/full-depth fill not modelled. Unknown fees keep net PnL NULL.',
            'legacy_scope':('Continuous author market snapshots, source-clock mid prices only; receive times/executable spread unavailable.' if clock=='source-mark' else 'Only continuous live-capture sources; selected trade windows and labelled winning exports excluded.')}
    if engine.gate:
        result['latency_gate']={'counts':engine.state['gate_counts'],'scope':'Timing-only shadow decisions. All alerts and fixed-config-delay counterfactual outcomes, including suppressed alerts, are retained. Not a profit model.',
            'outcomes':[dict(x) for x in c.execute("SELECT json_extract(s.features_json,'$.latency_gate.status') gate_status,o.horizon_ms,o.status,COUNT(*) n,AVG(o.markout_bps) mean_markout_bps FROM detector_outcomes o JOIN detector_signals s ON s.run_id=o.run_id AND s.event_key=o.signal_key WHERE o.run_id=? GROUP BY gate_status,o.horizon_ms,o.status",(run_id,))]}
    if split:
        result['chronological_split']={'split_ms':split,'train_signals':c.execute('SELECT COUNT(*) FROM detector_signals WHERE run_id=? AND recv_ms<?',(run_id,split)).fetchone()[0],
                                       'test_signals':c.execute('SELECT COUNT(*) FROM detector_signals WHERE run_id=? AND recv_ms>=?',(run_id,split)).fetchone()[0],
                                       'note':'Config stays fixed; this command performs no parameter search.'}
    return result


def begin(c,run_id,mode,config,start,end):
    ch=digest(config);code=code_hash();prior=c.execute('SELECT * FROM detector_runs WHERE id=?',(run_id,)).fetchone()
    if prior:
        if mode!='shadow' or prior['config_sha256']!=ch or prior['code_sha256']!=code or prior['mode']!=mode:
            raise ValueError('Run already exists or config/code changed; choose a new run id')
        return
    with c:c.execute('INSERT INTO detector_runs VALUES(?,?,?,?,?,?,?,?,?,?)',
                    (run_id,mode,time.time()*1000,canonical(config),ch,code,start,end,'RUNNING',None))


def replay(root,config,start,end,clock='available',run_id=None,split=None):
    if end<=start:raise ValueError('until must be after since')
    config=validate(config);include_pressure=bool((config.get('latency_gate') or {}).get('enabled'))
    if clock=='source-mark' and not config.get('allow_mid_quotes'):raise ValueError('source-mark requires explicit allow_mid_quotes config; execution quotes are unavailable')
    run_id=run_id or 'replay-'+dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%S')+'-'+uuid.uuid4().hex[:6]
    db=root/'data/unified/market.sqlite3';writer=connect(db);init(writer);reader=connect(db,readonly=True)
    begin(writer,run_id,'replay',config,start,end);engine=Detector(config);signals=[];outcomes=[]
    for event in events(reader,start,end,clock,include_pressure=include_pressure):
        a,b=engine.on(event);signals.extend(a);outcomes.extend(b)
        if len(signals)+len(outcomes)>=100:
            with writer:save(writer,run_id,signals,outcomes)
            signals=[];outcomes=[]
    for pending in engine.state['pending']:
        for horizon in config['horizons_ms']+[-1]:
            if horizon in pending['done']:continue
            outcomes.append({'signal_key':pending['key'],'horizon_ms':horizon,'observed_ms':end,'markout_bps':None,
                             'adverse_bps':pending['adverse_bps'],'favorable_bps':pending['favorable_bps'],
                             'status':'censored_end_of_window','details':{'entry':pending.get('entry'),'hedge_entry':pending.get('hedge_entry')}})
    with writer:save(writer,run_id,signals,outcomes)
    result=summary(writer,run_id,engine,clock,split);result.update(start_ms=start,end_ms=end,status='COMPLETE' if engine.state['seen'] else 'NO_DATA')
    with writer:writer.execute('UPDATE detector_runs SET status=?,summary_json=? WHERE id=?',(result['status'],canonical(result),run_id))
    folder=root/'reports/replay'/run_id;folder.mkdir(parents=True,exist_ok=False)
    (folder/'manifest.json').write_text(json.dumps(result,ensure_ascii=False,indent=2));(folder/'config.json').write_text(json.dumps(config,indent=2))
    for table,name in [('detector_signals','signals'),('detector_outcomes','outcomes')]:
        with (folder/(name+'.jsonl')).open('w') as f:
            for row in writer.execute('SELECT * FROM '+table+' WHERE run_id=?',(run_id,)):f.write(canonical(dict(row))+'\n')
    reader.close();writer.close();return result


def shadow(root,config,run_id):
    config=validate(config);include_pressure=bool((config.get('latency_gate') or {}).get('enabled'))
    folder=root/'data/unified';lock=(folder/'shadow.lock').open('a');fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    writer=connect(folder/'market.sqlite3');init(writer);reader=connect(folder/'market.sqlite3',readonly=True)
    start=time.time()*1000-120000;begin(writer,run_id,'shadow',config,start,None)
    prior=writer.execute('SELECT json FROM research_state WHERE name=?',('detector/'+run_id,)).fetchone()
    state=json.loads(prior[0]) if prior else {};engine=Detector(config,state.get('engine'));after=state.get('after')
    if after:start=after[0]
    stop=False
    def stopping(*_):
        nonlocal stop
        stop=True
    for sig in [signal.SIGINT,signal.SIGTERM]:signal.signal(sig,stopping)
    last_status=0
    while not stop:
        batch=list(available(reader,start,time.time()*1000,after=after,limit=6000,include_pressure=include_pressure))
        signals=[];outcomes=[]
        for event in batch:
            a,b=engine.on(event);signals.extend(a);outcomes.extend(b)
            after=[event['available_ms'],event['record_id'],event['sub']]
        if batch:
            with writer:
                save(writer,run_id,signals,outcomes,observations=True)
                writer.execute('INSERT OR REPLACE INTO research_state VALUES(?,?,?)',
                               ('detector/'+run_id,canonical({'engine':engine.state,'after':after}),time.time()*1000))
        if time.monotonic()-last_status>=1:
            result=summary(writer,run_id,engine,'available');result.update(mode='SHADOW_NO_ORDERS',now_ms=time.time()*1000,
                      pipeline_last_event_age_ms=time.time()*1000-engine.state['last_time'] if engine.state['last_time'] else None)
            with writer:writer.execute('UPDATE detector_runs SET summary_json=? WHERE id=?',(canonical(result),run_id))
            p=folder/'research_status.json';q=p.with_suffix('.new');q.write_text(json.dumps(result,ensure_ascii=False,indent=2));q.replace(p)
            last_status=time.monotonic()
        if not batch:time.sleep(.05)
    with writer:writer.execute("UPDATE detector_runs SET status='PAUSED' WHERE id=?",(run_id,))
    reader.close();writer.close()


def quality(root,lookback_ms=60000):
    c=connect(root/'data/unified/market.sqlite3',readonly=True);start=time.time()*1000-lookback_ms
    groups={};lag=[];mono=0;rows=0
    for r in c.execute('SELECT e.*,r.recv_ms,s.venue_hint,cs.boot_id,cs.connection FROM event_clock e JOIN records r ON r.id=e.record_id JOIN sources s ON s.id=r.source_id LEFT JOIN capture_sessions cs ON cs.id=e.session_id WHERE e.ingested_ms>=? AND s.role=\'live\'',(start,)):
        rows+=1
        if r['recv_ms']:lag.append(r['ingested_ms']-r['recv_ms'])
        if r['recv_mono_ns'] is not None:mono+=1
        if r['local_seq'] is not None:groups.setdefault((r['boot_id'],r['connection']),[]).append((r['local_seq'],r['recv_ms'],r['recv_mono_ns']))
    gaps=0;steps=0
    for values in groups.values():
        values.sort()
        for a,b in zip(values,values[1:]):
            if b[0]!=a[0]+1:gaps+=1
            if a[2] is not None and b[2] is not None and abs((b[1]-a[1])-(b[2]-a[2])/1e6)>100:steps+=1
    lag.sort()
    result={'window_ms':lookback_ms,'records':rows,'monotonic_clock_records':mono,'capture_connections':len(groups),
            'local_sequence_discontinuities':gaps,'clock_steps_over_100ms':steps,
            'receive_to_sql_ms':{k:lag[min(int((len(lag)-1)*q),len(lag)-1)] if lag else None for k,q in [('p50',.5),('p95',.95),('p99',.99)]},
            'scope':'Local capture sequence only; provider quote update IDs may jump legitimately. A clean sample does not prove loss-free exchange coverage.'}
    c.close();return result


def latency(root,since=None,until=None,limit=20000):
    """Bounded evidence only: transport measurements and labelled legacy proxies."""
    now=time.time()*1000;since=now-3600000 if since is None else since;until=now if until is None else until
    c=connect(root/'data/unified/market.sqlite3',readonly=True)
    try:
        lifecycle,rows=read_window(c,since,until,limit)
        positions={}
        kinds=('entry_decided','entry_filled','exit_decided','close_http_ack','exit_filled')
        for row in rows:
            r=dict(row)
            if r['origin']!='bot_db' or r['mode']!='live' or not r['position_id'] or r['kind'] not in kinds:continue
            phases=positions.setdefault(r['position_id'],{})
            phases.setdefault(r['kind'],[]).append({'event_id':r['id'],'event_ms':r['event_ms'],'precision':r['precision']})
        results=[]
        for pid,phases in positions.items():
            out={'position_id':pid,'phase_evidence':phases,'quality':['CLOCK_DOMAINS_UNVERIFIED']}
            for kind,values in phases.items():
                times={v['event_ms'] for v in values if finite(v['event_ms'])}
                if any(not finite(v['event_ms']) for v in values):out['quality'].append('INVALID_'+kind.upper()+'_CLOCK')
                out[kind]=next(iter(times)) if len(times)==1 else None
                if len(times)>1:out['quality'].append('MULTIPLE_'+kind.upper()+'_VERSIONS')
            for label,a,b in [('decision_to_entry_ms','entry_decided','entry_filled'),('decision_to_exit_ms','exit_decided','exit_filled'),('close_ack_to_exit_ms','close_http_ack','exit_filled')]:
                out[label]=None
                value=out[b]-out[a] if out.get(a) is not None and out.get(b) is not None else None
                if value is not None and value<0:out['quality'].append('NEGATIVE_'+label.upper()+'_PROXY');value=None
                out['proxy_'+label]=value
            out['clock_scope']='Legacy position joins are not attempt identities; proxy differences may mix provider/local clocks and are not network RTT.'
            results.append(out)
        economics=[dict(x) for x in c.execute('SELECT position_id,kind,instrument,source_ms,pnl_raw,fee_raw,paper_raw,tx_hash FROM execution_facts WHERE observed_ms>=? AND observed_ms<=? ORDER BY observed_ms DESC,event_id DESC LIMIT ?',(since,until,limit))]
        return {'positions':results,'provider_execution_facts':economics,
            'client_transport_requests':{a['request_id']:a for a in lifecycle['attempts']},'lifecycle':lifecycle,
            'exact_request_send_available':lifecycle['exact_request_send_available'],
            'raw_amount_scale':'Source integer amounts; adjustedPnl and fee are separate, do not subtract twice.',
            'scope':'Bounded read-only projection. Missing or incomparable phases remain NULL. Legacy proxy_* fields are NOT measured execution latency. HTTP acknowledgement is not cancellation.'}
    finally:c.close()


def main():
    p=argparse.ArgumentParser();p.add_argument('command',choices=['replay','shadow','status','quality','latency','lifecycle'])
    p.add_argument('--project-root',type=Path,default=DEFAULT_ROOT);p.add_argument('--config',type=Path,default=Path(__file__).resolve().parents[1]/'configs/lead-lag.json')
    p.add_argument('--since');p.add_argument('--until');p.add_argument('--split-at');p.add_argument('--clock',choices=['available','receive','source-mark'],default='available');p.add_argument('--run-id')
    p.add_argument('--limit',type=int,default=20000,help='Bounded lifecycle evidence rows, maximum 100000')
    a=p.parse_args();now=time.time()*1000
    if a.command=='quality':result=quality(a.project_root)
    elif a.command in ('latency','lifecycle'):
        result=latency(a.project_root,ms(a.since,now-3600000),ms(a.until,now),a.limit)
        if a.command=='lifecycle':result=result['lifecycle']
    elif a.command=='status':result=json.loads((a.project_root/'data/unified/research_status.json').read_text())
    else:
        config=validate(json.loads(a.config.read_text()))
        if a.command=='shadow':shadow(a.project_root,config,a.run_id or 'shadow-lead-lag-v1');return
        result=replay(a.project_root,config,ms(a.since,now-300000),ms(a.until,now),a.clock,a.run_id,ms(a.split_at,None))
    print(json.dumps(result,ensure_ascii=False,indent=2))


if __name__=='__main__':main()
