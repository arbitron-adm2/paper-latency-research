"""Read-only wallet SSE, account history, local decisions and durable trade changes.

No signer, private runtime key, order method or control POST is used here.
"""
import argparse
import asyncio
import fcntl
import json
import signal
import sqlite3
import time
import urllib.parse
from pathlib import Path
import aiohttp
from .core import DEFAULT_ROOT
from .evidence import clean, digest, row_phases
from .supplement import Supplement
from .wire import BOOT_ID


class Collector:
    def __init__(self,root,wallet,bot_url='http://127.0.0.1:18762'):
        self.root=Path(root);self.wallet=wallet.lower();self.bot_url=bot_url
        self.directory=self.root/'data/unified/telemetry';self.journal=Supplement(self.directory,capture_profile={'name':'read_only_execution_evidence'})
        self.stop=asyncio.Event();self.seen={};self.connection=str(time.time_ns());self.seq=0
        self.health={};self.counts={};self.errors={};self.history_complete=False
        self.bot_pid=None

    def emit(self,origin,kind,payload,event_ms=None,precision='observed_snapshot',intent_id=None,position_id=None,mode='live',identity=None):
        payload=clean(payload);key=digest([origin,kind,identity if identity is not None else payload,event_ms])
        # Bound in-memory deduplication; SQLite unique keys remain authoritative after restart.
        if key in self.seen:return
        self.seen[key]=True
        if len(self.seen)>30000:self.seen.pop(next(iter(self.seen)))
        e={'origin':origin,'kind':kind,'payload':payload,'event_ms':event_ms,'precision':precision,
           'wallet':self.wallet,'intent_id':intent_id,'position_id':position_id,'mode':mode,'key':key}
        self.seq+=1
        self.journal.save(origin,'research',json.dumps(e,separators=(',',':'),allow_nan=False),
                          capture={'connection':self.connection,'seq':self.seq,'boot_id':BOOT_ID})
        self.counts[origin]=self.counts.get(origin,0)+1

    def error(self,origin,exc):
        self.errors[origin]={'type':type(exc).__name__,'at_ms':time.time()*1000,
                            'count':self.errors.get(origin,{}).get('count',0)+1}
        self.emit('capture_health','collector_error',{'source':origin,'type':type(exc).__name__},time.time()*1000,'local_wall')

    async def wallet_stream(self,http):
        url='https://exchange.papertrade.xyz/state/user/live?wallet='+self.wallet
        attempt=0
        while not self.stop.is_set():
            try:
                self.emit('capture_health','wallet_connect',{'attempt':attempt},time.time()*1000,'local_wall')
                async with http.get(url,headers={'Accept':'text/event-stream'},timeout=aiohttp.ClientTimeout(total=45,sock_read=20)) as resp:
                    resp.raise_for_status();name='';data=[]
                    async for raw in resp.content:
                        line=raw.decode().rstrip('\r\n')
                        if not line:
                            if data and name=='wallet':
                                j=json.loads('\n'.join(data));self.health['wallet_last_ms']=time.time()*1000
                                self.emit('paper_wallet','wallet_event',j,precision='provider_state_observed')
                                for op in j.get('addedOperations',j.get('operations',[])):
                                    self.emit('paper_wallet','operation_seen',{'operation':op},float(op[7]) if len(op)>7 else None,
                                              'provider_operation_time',intent_id=str(op[0]))
                                for p in j.get('addedPositions',j.get('openPositions',[])):
                                    self.emit('paper_wallet','position_seen',{'position':p},float(p[8]) if len(p)>8 else None,
                                              'provider_position_time',intent_id=str(p[9]) if len(p)>9 else None,position_id=str(p[0]))
                            name='';data=[]
                        elif line.startswith('event:'):name=line[6:].strip()
                        elif line.startswith('data:'):data.append(line[5:].lstrip())
                        if self.stop.is_set():break
            except Exception as exc:self.error('wallet',exc)
            attempt+=1
            try:await asyncio.wait_for(self.stop.wait(),1)
            except asyncio.TimeoutError:pass

    async def history(self,http):
        cursor=None
        while not self.stop.is_set():
            try:
                params={'cursor':cursor} if cursor else {}
                async with http.get('https://exchange.papertrade.xyz/query/accounts/'+self.wallet+'/trades',params=params,
                                    timeout=aiohttp.ClientTimeout(total=8)) as resp:
                    resp.raise_for_status();j=await resp.json()
                if not isinstance(j.get('items'),list):raise ValueError('History items missing')
                for item in j['items']:
                    self.emit('paper_account_history',item.get('kind','unknown'),item,item.get('timestampMs'),
                              'provider_execution_time',position_id=str(item.get('positionId')),intent_id=item.get('intentId'))
                nxt=j.get('nextCursor');self.health['history_last_ms']=time.time()*1000
                self.emit('capture_health','history_page',{'cursor':cursor,'next_cursor':nxt,'items':len(j['items']),'terminal':not nxt},
                          precision='coverage_metadata')
                if nxt and nxt==cursor:raise ValueError('History cursor did not advance')
                self.history_complete=not bool(nxt);cursor=nxt
                delay=1 if nxt else 30
            except Exception as exc:self.error('history',exc);delay=30
            try:await asyncio.wait_for(self.stop.wait(),delay)
            except asyncio.TimeoutError:pass

    async def bot_state(self,http):
        while not self.stop.is_set():
            try:
                async with http.get(self.bot_url+'/state',timeout=aiohttp.ClientTimeout(total=2)) as resp:
                    resp.raise_for_status();j=await resp.json()
                self.health['bot_last_ms']=time.time()*1000
                self.bot_pid=j.get('pid')
                mode='live' if j.get('LIVE') else 'disabled'
                self.emit('bot_state','config',{'pid':j.get('pid'),'config':j.get('config'),'protections':j.get('protectionSpecification')},mode=mode)
                for strategy,d in j.get('decisions',{}).items():
                    self.emit('bot_state','decision',{'strategy':strategy,'decision':d,'context_observed_ms':time.time()*1000,
                              'feeds':j.get('feeds'),'regime':j.get('regime')},d.get('t'),'local_decision_time_observed_later',
                              mode=mode,identity=[j.get('pid'),strategy,d])
                for ev in j.get('events',[]):self.emit('bot_state','bot_event',ev,ev.get('t'),'local_event_time_observed_later',mode=mode)
                self.emit('bot_state','managed_positions',{'positions':j.get('openManaged',[])},mode=mode)
                self.emit('bot_state','guard',{'guard':j.get('guard'),'timer':j.get('timer'),'highWater':j.get('highWater')},mode=mode,
                          identity=[j.get('guard'),j.get('LIVE'),j.get('entriesEnabled')])
            except Exception as exc:self.error('bot',exc)
            try:await asyncio.wait_for(self.stop.wait(),.5)
            except asyncio.TimeoutError:pass

    async def runtime_rows(self):
        path=self.root/'apps/vchera2/.runtime/vchera2.sqlite'
        src=None;version=None;inode=None
        while not self.stop.is_set():
            try:
                current_inode=path.stat().st_ino
                if src is not None and inode!=current_inode:src.close();src=None;version=None
                if src is None:
                    src=sqlite3.connect(path.resolve().as_uri()+'?mode=ro',uri=True,timeout=.25);src.row_factory=sqlite3.Row
                    inode=current_inode
                current=src.execute('PRAGMA data_version').fetchone()[0]
                if current!=version:
                    rows=src.execute('SELECT * FROM btcy2_lag_trades').fetchall()
                    for raw in rows:
                        row=dict(raw);phases=row_phases(row);mode='live' if row.get('live') else 'paper'
                        self.emit('bot_db','trade_row',row,precision='durable_row_observed',position_id=row.get('position_id'),intent_id=row.get('intent_id'),mode=mode)
                        for kind,stamp,payload in phases:
                            self.emit('bot_db',kind,payload,stamp,'recorded_local_or_provider_time',
                                      position_id=row.get('position_id'),intent_id=row.get('intent_id'),mode=mode,
                                      identity=[row['id'],kind,stamp,payload])
                    if src.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='btcy2_execution_audit'").fetchone():
                        for row in src.execute('SELECT * FROM btcy2_execution_audit'):
                            self.emit('bot_db','execution_audit',dict(row),row['at'],'durable_audit_time',position_id=row['position_id'])
                    version=current
                self.health['runtime_last_ms']=time.time()*1000
            except Exception as exc:self.error('runtime',exc)
            try:await asyncio.wait_for(self.stop.wait(),.5)
            except asyncio.TimeoutError:pass
        if src:src.close()

    async def client_events(self):
        offsets={};folder=self.root/'data/unified/client-events'
        while not self.stop.is_set():
            for path in folder.glob('client-*.jsonl'):
                try:
                    offset=offsets.get(str(path),0)
                    if path.stat().st_size<offset:offset=0
                    with path.open('rb') as f:
                        f.seek(offset)
                        for _ in range(2000):
                            start=f.tell();line=f.readline()
                            if not line or not line.endswith(b'\n'):f.seek(start);break
                            obj=json.loads(line);e=obj['data'];payload={**e['payload'],
                                'source_recv_mono_ns':obj.get('recv_mono_ns'),'source_capture':obj.get('_capture')}
                            self.emit('client_transport',e['kind'],payload,e.get('event_ms'),e.get('precision'),
                                      intent_id=payload.get('intent_id'),identity=e)
                            if e['kind'] in ['client_hook_started','client_hook_heartbeat']:
                                self.health['client_hook_last_ms']=float(obj['recv_ms']);self.health['client_hook_pid']=payload.get('pid')
                        offsets[str(path)]=f.tell()
                except Exception as exc:self.error('client_journal',exc)
            try:await asyncio.wait_for(self.stop.wait(),.25)
            except asyncio.TimeoutError:pass

    async def market_context(self,http):
        while not self.stop.is_set():
            try:
                for symbol in ['BTCUSDT','ETHUSDT']:
                    async with http.get('https://fapi.binance.com/fapi/v1/premiumIndex',params={'symbol':symbol},timeout=aiohttp.ClientTimeout(total=8)) as resp:
                        resp.raise_for_status();j=await resp.json()
                    self.emit('binance_context','mark_index_funding',j,j.get('time'),'provider_market_time',mode='market_context')
                async with http.post('https://api.hyperliquid.xyz/info',json={'type':'metaAndAssetCtxs'},timeout=aiohttp.ClientTimeout(total=8)) as resp:
                    resp.raise_for_status();j=await resp.json()
                for meta,ctx in zip(j[0]['universe'],j[1]):
                    if meta['name'] in ['BTC','ETH']:
                        self.emit('hl_context','mark_oracle_funding',{'instrument':meta,'context':ctx},precision='observed_snapshot',mode='market_context')
                self.health['market_context_last_ms']=time.time()*1000
            except Exception as exc:self.error('market_context',exc)
            try:await asyncio.wait_for(self.stop.wait(),60)
            except asyncio.TimeoutError:pass

    async def status(self):
        while not self.stop.is_set():
            now=time.time()*1000
            d={'now_ms':now,'health':self.health,'counts':self.counts,'errors':self.errors,'history_terminal_reached':self.history_complete,
               'bot_poll_ms':500,'exact_client_request_send_hook_active':self.health.get('client_hook_pid')==self.bot_pid and now-self.health.get('client_hook_last_ms',0)<30000,'wallet':self.wallet,
               'scope':'Read-only provider SSE/history, observed bot decisions and durable lifecycle rows; no client send time inferred.'}
            p=self.directory/'collector_status.json';q=p.with_suffix('.new');q.write_text(json.dumps(d,indent=2));q.replace(p)
            try:await asyncio.wait_for(self.stop.wait(),1)
            except asyncio.TimeoutError:pass

    async def run(self):
        lock=(self.directory/'collector.lock').open('a');fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        loop=asyncio.get_running_loop()
        for sig in [signal.SIGINT,signal.SIGTERM]:loop.add_signal_handler(sig,self.stop.set)
        async with aiohttp.ClientSession(headers={'User-Agent':'Mozilla/5.0','Accept':'application/json'}) as http:
            tasks=[asyncio.create_task(x) for x in [self.wallet_stream(http),self.history(http),self.bot_state(http),
                   self.runtime_rows(),self.client_events(),self.market_context(http),self.status(),self.journal.monitor()]]
            await self.stop.wait();self.journal.stop.set()
            for t in tasks:t.cancel()
            await asyncio.gather(*tasks,return_exceptions=True)
            for name in list(self.journal.active):self.journal.close_chunk(name)


def main():
    p=argparse.ArgumentParser();p.add_argument('--project-root',type=Path,default=DEFAULT_ROOT)
    p.add_argument('--wallet',default='0x1111111111111111111111111111111111111111');p.add_argument('--bot-url',default='http://127.0.0.1:18762')
    a=p.parse_args();asyncio.run(Collector(a.project_root,a.wallet,a.bot_url).run())


if __name__=='__main__':main()
