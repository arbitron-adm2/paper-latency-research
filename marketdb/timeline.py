"""Streaming causal timelines: SQL availability now, receive-clock legacy history."""
import heapq
import itertools
import json
import zlib
from .core import connect

MARKETS=('hl','binance_futures','binance_spot')


def _available_market(c,start,end,after=None,limit=None):
    clauses=["e.ingested_ms>=?","e.ingested_ms<=?","s.role='live'",
             "s.venue_hint IN ('hl','binance_futures','binance_spot')","t.kind IN ('bbo','trade')"]
    args=[start,end]
    if after:
        clauses.append('(e.ingested_ms,t.record_id,t.sub)>(?,?,?)');args.extend(after)
    sql=('SELECT t.*,e.ingested_ms AS available_ms,e.recv_mono_ns,e.local_seq,e.provider_seq,e.source_event_ms,e.source_trade_ms,cs.boot_id,cs.connection '
         'FROM event_clock e JOIN ticks t ON t.record_id=e.record_id JOIN sources s ON s.id=t.source_id '
         'LEFT JOIN capture_sessions cs ON cs.id=e.session_id WHERE '+' AND '.join(clauses)+
         ' ORDER BY e.ingested_ms,t.record_id,t.sub')
    if limit:sql+=' LIMIT ?';args.append(limit)
    cursor=c.execute(sql,args)
    try:
        for row in cursor:
            d=dict(row);d['time_ms']=d['available_ms'];yield d
    finally:cursor.close()


def _received_market(c,start,end):
    """Only continuous live sources; never silently mix selected trade windows."""
    sources=c.execute("SELECT id,last_recv_ms FROM sources WHERE role='live' AND venue_hint IN ('hl','binance_futures','binance_spot') AND last_recv_ms>=?",(start,)).fetchall()
    heap=[];cursors=[]
    def advance(cur,sid):
        r=cur.fetchone()
        if r is not None:
            d=dict(r);d['time_ms']=d['recv_ms'];heapq.heappush(heap,(d['time_ms'],d['record_id'],d['sub'],sid,d,cur))
    try:
        for s in sources:
            sid=s['id'];lo=sid<<40;hi=((sid+1)<<40)-1
            first=c.execute('SELECT recv_ms FROM records WHERE id BETWEEN ? AND ? ORDER BY id LIMIT 1',(lo,hi)).fetchone()
            if first and first[0] is not None and first[0]>end:continue
            cur=c.execute("SELECT * FROM ticks WHERE record_id BETWEEN ? AND ? AND recv_ms>=? AND recv_ms<=? AND kind IN ('bbo','trade') ORDER BY record_id,sub",(lo,hi,start,end))
            cursors.append(cur);advance(cur,sid)
        while heap:
            _,_,_,sid,d,cur=heapq.heappop(heap);yield d;advance(cur,sid)
    finally:
        for cur in cursors:cur.close()


def _pressure(c,start,end,clock,after=None):
    """One last pressure snapshot per frame; no signals can occur inside a frame."""
    stamp='e.ingested_ms' if clock=='available' else 'r.recv_ms'
    join=' JOIN event_clock e ON e.record_id=r.id' if clock=='available' else ''
    clauses=[stamp+'>=?',stamp+'<=?',"s.role='live'", "s.venue_hint='paper'", "r.kind IN ('pressure','trading')"]
    args=[start,end]
    if after:
        clauses.append('('+stamp+',r.id,-1)>(?,?,?)');args.extend(after)
    sql=('SELECT r.id,r.source_id,r.kind,r.recv_ms,r.raw_z,'+stamp+' AS time_ms FROM records r'
         +join+' JOIN sources s ON s.id=r.source_id WHERE '+' AND '.join(clauses)+' ORDER BY '+stamp+',r.id')
    cursor=c.execute(sql,args)
    try:
        for row in cursor:
            snapshot=None;index=None
            try:
                obj=json.loads(zlib.decompress(row['raw_z']));data=obj['data']
                pubs=list(data.get('subscribe',{}).get('publications',[]))
                if data.get('push',{}).get('pub'):pubs.append(data['push']['pub'])
                if not pubs:pubs=[{'data':data}]
                for i,pub in enumerate(pubs):
                    value=pub.get('data')
                    if isinstance(value,dict):
                        snap=value.get('snapshot',value)
                        if isinstance(snap,dict) and ('totalPendingGas' in snap or 'chain' in snap):snapshot=snap;index=i
            except (ValueError,TypeError,KeyError,AttributeError,zlib.error):pass
            if snapshot is None and row['kind']!='pressure':continue
            yield {'record_id':row['id'],'sub':-1,'source_id':row['source_id'],'symbol':None,'venue':'paper',
                   'kind':'pressure','time_ms':row['time_ms'],'available_ms':row['time_ms'] if clock=='available' else None,
                   'recv_ms':row['recv_ms'],'source_ms':snapshot.get('ts') if snapshot else None,
                   'pressure':snapshot,'publication_index':index}
    finally:cursor.close()


def _merge_context(market,pressure,limit=None):
    try:
        merged=heapq.merge(market,pressure,key=lambda e:(e['time_ms'],e['record_id'],e['sub']))
        yield from itertools.islice(merged,limit) if limit is not None else merged
    finally:
        market.close();pressure.close()


def available(c,start,end,after=None,limit=None,include_pressure=False):
    if not include_pressure:return _available_market(c,start,end,after,limit)
    return _merge_context(_available_market(c,start,end,after),_pressure(c,start,end,'available',after),limit)


def received(c,start,end,include_pressure=False):
    if not include_pressure:return _received_market(c,start,end)
    return _merge_context(_received_market(c,start,end),_pressure(c,start,end,'receive'))


def events(c,start,end,clock='available',include_pressure=False):
    if clock=='available':return available(c,start,end,include_pressure=include_pressure)
    if clock=='receive':return received(c,start,end,include_pressure=include_pressure)
    if clock=='source-mark':return historical_marks(c,start,end)
    raise ValueError('clock must be available or receive; source clocks are not causal availability')


def historical_marks(c,start,end):
    """Vlad continuous snapshots: prices only, no invented receive times or spread."""
    heap=[];cursors=[]
    def advance(cur,sid):
        row=cur.fetchone()
        if row:
            d=dict(row);d.update(time_ms=d['ts_ms'],kind='mid',snapshot_final=d['sub']==1)
            heapq.heappush(heap,(d['time_ms'],d['record_id'],d['sub'],sid,d,cur))
    try:
        for s in c.execute("SELECT id FROM sources WHERE role='market' AND dataset='market_snapshot' AND max_ms>=? AND min_ms<=?",(start,end)).fetchall():
            sid=s['id'];cur=c.execute('SELECT * FROM ticks WHERE record_id BETWEEN ? AND ? AND ts_ms>=? AND ts_ms<=? ORDER BY record_id,sub',(sid<<40,((sid+1)<<40)-1,start,end))
            cursors.append(cur);advance(cur,sid)
        while heap:
            _,_,_,sid,d,cur=heapq.heappop(heap);yield d;advance(cur,sid)
    finally:
        for cur in cursors:cur.close()
