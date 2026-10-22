"""Only feeds missing from the existing observer: spot and USD-M market trades."""
import asyncio
import datetime as dt
import gzip
import json
import threading
import time
from pathlib import Path
import websockets
from .streams import FUTURES_TRADES_URL, SPOT_URL, DEPTH_ENABLED
from .wire import frame, BOOT_ID


class Supplement:
    def __init__(self, directory, capture_profile=None):
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.active = {}
        self.connected = {}
        self.errors = {}
        self.stop = threading.Event()
        self.run_id = str(time.time_ns())
        self.thread = threading.Thread(target=self.run, name='public-missing-feeds', daemon=True)
        self.capture_profile=capture_profile or {'name':'backtest_depth20' if DEPTH_ENABLED else 'prices_and_trades','orderbook_depth':DEPTH_ENABLED,
            'binance_futures_trades':FUTURES_TRADES_URL,'binance_spot':SPOT_URL}

    def health_event(self,kind,name,**extra):
        now=time.time()*1000
        e={'origin':'market_capture','kind':kind,'event_ms':now,'precision':'local_wall',
           'payload':{'source':name,**extra}}
        self.save('capture_health','research',json.dumps(e,separators=(',',':')))

    def close_chunk(self, name):
        a = self.active.pop(name, None)
        if a:
            a['file'].close()
            row = {k: v for k, v in a.items() if k != 'file'}
            with (self.directory/'manifest.jsonl').open('a') as f: f.write(json.dumps(row)+'\n')

    def save(self, name, venue, raw, capture=None):
        now = time.time()*1000; mono=time.monotonic_ns();bucket = int(now//300000)
        if name in self.active and self.active[name]['bucket'] != bucket: self.close_chunk(name)
        if name not in self.active:
            path = self.directory/f'{name}-{bucket}-{self.run_id}.jsonl.gz'
            self.active[name] = {'file': gzip.open(path, 'wb', compresslevel=1), 'path': str(path),
                'source': name, 'venue': venue, 'bucket': bucket, 'rows': 0, 'first_recv_ms': now, 'last_recv_ms': now}
        a = self.active[name]
        a['file'].write(frame(raw=raw, recv_ms=now, recv_mono_ns=mono, capture=capture))
        a['rows'] += 1; a['last_recv_ms'] = now

    async def socket(self, name, venue, url):
        retries = 0
        while not self.stop.is_set():
            try:
                async with websockets.connect(url, open_timeout=12, close_timeout=2, ping_interval=20, ping_timeout=20, max_size=2**24) as ws:
                    self.connected[name] = True
                    retries = 0
                    connection=str(time.time_ns());seq=0
                    self.health_event('connected',name,connection=connection,boot_id=BOOT_ID)
                    last_data = time.monotonic()
                    while not self.stop.is_set():
                        try: raw = await asyncio.wait_for(ws.recv(), 3)
                        except asyncio.TimeoutError:
                            if time.monotonic()-last_data > 45: raise TimeoutError('No websocket messages for 45 seconds')
                            continue
                        last_data = time.monotonic()
                        seq+=1
                        self.save(name, venue, raw, capture={'connection':connection,'seq':seq,'boot_id':BOOT_ID})
            except Exception as exc:
                self.health_event('socket_error',name,type=type(exc).__name__)
                self.errors[name] = {'count': self.errors.get(name, {}).get('count', 0)+1,
                                     'type': type(exc).__name__, 'at_ms': time.time()*1000}
            finally:
                self.connected[name] = False
                self.health_event('disconnected',name)
            retries += 1
            for _ in range(min(2**min(retries, 5), 30)):
                if self.stop.is_set(): break
                await asyncio.sleep(1)

    async def monitor(self):
        last_status=0
        while not self.stop.is_set():
            for a in self.active.values(): a['file'].flush()
            state = {'now_ms': time.time()*1000, 'connected': self.connected, 'errors': self.errors,
                     'capture_profile': self.capture_profile,
                     'active_chunks': [{k:v for k,v in a.items() if k != 'file'} for a in self.active.values()]}
            if time.monotonic()-last_status>=1:
                p = self.directory/'status.json'; q = p.with_suffix('.new')
                q.write_text(json.dumps(state, indent=2)); q.replace(p);last_status=time.monotonic()
            await asyncio.sleep(.25)

    async def main(self):
        futures = FUTURES_TRADES_URL
        spot = SPOT_URL
        tasks = [asyncio.create_task(self.socket('binance_market_trades','binance_futures',futures)),
                 asyncio.create_task(self.socket('binance_spot','binance_spot',spot)),
                 asyncio.create_task(self.monitor())]
        while not self.stop.is_set(): await asyncio.sleep(.2)
        for t in tasks: t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        for name in list(self.active): self.close_chunk(name)

    def run(self): asyncio.run(self.main())
    def start(self): self.thread.start()
    def close(self): self.stop.set(); self.thread.join(6)
