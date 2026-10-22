"""Anonymous public market capture; no keys, order methods, or time limit."""
import asyncio,collections,datetime,fcntl,gzip,json,os,shutil,signal,time
from pathlib import Path
import websockets
from .streams import HL_URL, HL_SUBSCRIPTIONS, HL_AGG_SUBSCRIPTIONS, HL_FAST_SUBSCRIPTIONS, DEPTH_ENABLED, FUTURES_QUOTES_URL, FUTURES_DEPTH_URL, PAPER_URL, PAPER_SUBSCRIPTIONS, profile
from .wire import frame, BOOT_ID
from .core import DEFAULT_ROOT

OUT=None;CHUNKS=None;LOCK=None
BOOT=time.time();RUN=datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%S')+'-'+str(os.getpid())
ROTATE_S=300;RESERVE_BYTES=5*1024**3
STOP=asyncio.Event();COUNTS=collections.Counter();LAST={};SOURCE_TIME={};CONNECTED={};ERRORS=collections.Counter();ACTIVE={};RECONNECT={};START=None;STOP_REASON=None

def configure(directory):
 global OUT,CHUNKS,LOCK
 OUT=Path(directory).resolve();OUT.mkdir(parents=True,exist_ok=True)
 CHUNKS=OUT/'chunks';CHUNKS.mkdir(exist_ok=True)
 LOCK=(OUT/'observer.lock').open('a');fcntl.flock(LOCK,fcntl.LOCK_EX|fcntl.LOCK_NB)

def atomic_status(d):
 p=OUT/('status.'+str(os.getpid())+'.new');p.write_text(json.dumps(d,indent=2));os.replace(p,OUT/'status.json')

def close_chunk(source):
 a=ACTIVE.pop(source,None)
 if not a:return
 a['file'].close()
 with (OUT/'manifest.jsonl').open('a') as f:f.write(json.dumps({k:v for k,v in a.items() if k!='file'}|{'source':source,'compressed_bytes':(OUT/a['path']).stat().st_size})+'\n')

def save(source,data,raw=None,recv_ms=None,recv_mono_ns=None,capture=None):
 now=recv_ms/1000 if recv_ms is not None else time.time();bucket=int(now//ROTATE_S)*ROTATE_S;a=ACTIVE.get(source)
 if a is not None and a['bucket']!=bucket:close_chunk(source);a=None
 if a is None:
  stamp=datetime.datetime.fromtimestamp(bucket,datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ');name=source+'-'+stamp+'-'+RUN+'.jsonl.gz';path=CHUNKS/name
  a={'file':gzip.open(path,'xb',compresslevel=1),'path':str(path.relative_to(OUT)),'bucket':bucket,'first_recv_ms':now*1000,'last_recv_ms':now*1000,'rows':0,'run_id':RUN};ACTIVE[source]=a
 a['file'].write(frame(data,raw=raw,recv_ms=now*1000,recv_mono_ns=recv_mono_ns,capture=capture));a['last_recv_ms']=now*1000;a['rows']+=1;COUNTS[source]+=1

def event(kind,**kw):save('control',{'event':kind,**kw})
def seen(key,src=None):
 LAST[key]=time.time()
 if src is not None:SOURCE_TIME[key]=src

def on_hl(source,d):
 ch=d.get('channel');x=d.get('data',{})
 if ch in ['bbo','l2Book']:seen(source+'_'+ch+'_'+x['coin'],x.get('time'))
 elif ch=='trades' and isinstance(x,list) and x:seen(source+'_trades_'+x[0]['coin'],max(t['time'] for t in x))
 elif ch=='error':ERRORS[source]+=1;event('upstream_error',source=source,detail=x)

def on_bin(d):
 x=d.get('data',{});sym=x.get('s','').replace('USDT','');st=d.get('stream','')
 for token,typ in [('@depth','depth'),('@bookTicker','bbo'),('@aggTrade','trades')]:
  if token in st:seen('binance_'+typ+'_'+sym,x.get('T',x.get('E')));break

def on_paper(d):
 if d.get('error'):ERRORS['paper']+=1;event('upstream_error',source='paper',detail=d)
 if d.get('id') in [2,3,4] and 'subscribe' in d:seen('paper_subscription_'+str(d['id']))
 pubs=list(d.get('subscribe',{}).get('publications',[]))
 if d.get('push',{}).get('pub'):pubs.append(d['push']['pub'])
 for pub in pubs:
  x=pub.get('data',{})
  if isinstance(x,list):seen('paper_trades',max((a.get('ts',0) for a in x),default=0))
  elif isinstance(x,dict) and 'snapshot' in x:
   s=x['snapshot'];seen('paper_'+('pressure' if 'chain' in s else 'trading'),s.get('ts'))
  elif isinstance(x,dict):seen('paper_trading')

async def ping_hl(w):
 while not STOP.is_set():
  await asyncio.sleep(20);await w.send('{"method":"ping"}')

async def run_socket(name,url,subscriptions,origin=None):
 attempt=0;failure_streak=0
 while not STOP.is_set():
  ping=None
  try:
   async with websockets.connect(url,origin=origin,open_timeout=12,close_timeout=2,ping_interval=20,ping_timeout=15,max_size=2**24) as w:
    connection=str(time.time_ns());seq=0
    stale_source_since=None
    CONNECTED[name]=True;event('connected',source=name,attempt=attempt,connection=connection,boot_id=BOOT_ID);RECONNECT[name]=attempt
    for sub in subscriptions:await w.send(json.dumps(sub))
    if name.startswith('hl'):ping=asyncio.create_task(ping_hl(w))
    last_data=time.monotonic();connection_started=last_data
    while not STOP.is_set():
     try:raw=await asyncio.wait_for(w.recv(),3)
     except asyncio.TimeoutError:
      if time.monotonic()-last_data>45:raise TimeoutError('No websocket messages for 45 seconds')
      continue
     last_data=time.monotonic()
     recv_ms=time.time()*1000;recv_mono_ns=time.monotonic_ns()
     if isinstance(raw,bytes):raw=raw.decode()
     for line in raw.split('\n'):
      if not line:continue
      d=json.loads(line)
      if name=='paper' and d=={}:await w.send('{}');continue
      seq+=1
      save('binance' if name=='binance_depth' else name,d,raw=line,recv_ms=recv_ms,recv_mono_ns=recv_mono_ns,capture={'connection':connection,'seq':seq,'boot_id':BOOT_ID,'transport':name})
      if name.startswith('hl'):on_hl(name,d)
      elif name.startswith('binance'):
       on_bin(d)
       stamp=d.get('data',{}).get('E')
       if stamp and recv_ms-float(stamp)>5000:
        if stale_source_since is None:stale_source_since=time.monotonic()
        elif time.monotonic()-stale_source_since>2:raise TimeoutError('Stale Binance event time persisted beyond 5 seconds')
       else:
        stale_source_since=None
        if time.monotonic()-connection_started>=30:failure_streak=0
      else:on_paper(d)
  except asyncio.CancelledError:raise
  except OSError as exc:
   if exc.errno in [28,122]:raise
   ERRORS[name]+=1;event('socket_error',source=name,type=type(exc).__name__,detail=str(exc)[:300])
  except Exception as exc:
   ERRORS[name]+=1;event('socket_error',source=name,type=type(exc).__name__,detail=str(exc)[:300])
  finally:
   CONNECTED[name]=False
   if ping:ping.cancel();await asyncio.gather(ping,return_exceptions=True)
  attempt+=1;failure_streak+=1
  if not STOP.is_set():
   try:await asyncio.wait_for(STOP.wait(),min(2**min(failure_streak,5),30))
   except asyncio.TimeoutError:pass

def status():
 now=time.time();ages={k:round(now-v,3) for k,v in LAST.items()};required=['hl_bbo_BTC','hl_bbo_ETH','binance_bbo_BTC','binance_bbo_ETH','paper_pressure','paper_subscription_3'];stale=[k for k in required if k not in ages or (not k.startswith('paper_subscription') and ages[k]>5)]
 if DEPTH_ENABLED:
  required += ['hl_l2Book_BTC','hl_l2Book_ETH','hl_agg_l2Book_BTC','hl_agg_l2Book_ETH','hl_fast_l2Book_BTC','hl_fast_l2Book_ETH','binance_depth_BTC','binance_depth_ETH']
  stale=[k for k in required if k not in ages or (not k.startswith('paper_subscription') and ages[k]>(10 if k.startswith(('hl_l2Book_','hl_agg_l2Book_')) else 5))]
 return {'run_id':RUN,'pid':os.getpid(),'mode':'continuous_read_only','capture_profile':profile(),'boot_ms':BOOT*1000,'now_ms':now*1000,'measurement_start_ms':START*1000 if START else None,'measurement_end_ms':None,'elapsed_s':now-START if START else None,'rotation_seconds':ROTATE_S,'counts':dict(COUNTS),'connected':dict(CONNECTED),'error_counts':dict(ERRORS),'reconnect_attempts':dict(RECONNECT),'last_age_s':ages,'last_source_ms':dict(SOURCE_TIME),'health':'STOPPED' if STOP.is_set() else 'HEALTHY' if not stale and all(CONNECTED.get(s) for s in ['hl','binance','paper']) else 'DEGRADED','missing_or_stale':stale,'active_chunks':[{k:v for k,v in a.items() if k!='file'}|{'source':s} for s,a in ACTIVE.items()],'disk_free_bytes':shutil.disk_usage(OUT).free,'stop_reason':STOP_REASON}

async def monitor(tasks):
 global START,STOP_REASON
 last_status=0
 while not STOP.is_set():
  for t in tasks:
   if t.done():
    await t;raise RuntimeError('A collector task unexpectedly ended')
  s=status()
  if START is None and s['health']=='HEALTHY':START=time.time();event('measurement_started',duration_s=None)
  if s['disk_free_bytes']<RESERVE_BYTES:
   STOP_REASON='disk_free_below_5GiB';event('resource_stop',reason=STOP_REASON);STOP.set()
  for a in ACTIVE.values():a['file'].flush()
  if time.monotonic()-last_status>=1:
   atomic_status(status());last_status=time.monotonic()
  try:await asyncio.wait_for(STOP.wait(),.25)
  except asyncio.TimeoutError:pass

async def main():
 global STOP_REASON
 loop=asyncio.get_running_loop()
 def stopping(sig):
  global STOP_REASON
  STOP_REASON=signal.Signals(sig).name;STOP.set()
 for sig in [signal.SIGTERM,signal.SIGINT]:loop.add_signal_handler(sig,stopping,sig)
 event('process_started',run_id=RUN,duration_s=None,rotation_seconds=ROTATE_S,capture_profile=profile())
 tasks=[asyncio.create_task(run_socket('hl',HL_URL,HL_SUBSCRIPTIONS)),asyncio.create_task(run_socket('binance',FUTURES_QUOTES_URL,[])),asyncio.create_task(run_socket('paper',PAPER_URL,PAPER_SUBSCRIPTIONS,'https://exchange.papertrade.xyz'))]
 if FUTURES_DEPTH_URL:tasks.append(asyncio.create_task(run_socket('binance_depth',FUTURES_DEPTH_URL,[])))
 if os.environ.get('PAPER_CAPTURE_PARAMETERS')=='1':
  from .parameters import poll
  tasks.append(asyncio.create_task(poll(save,event,STOP)))
 if HL_FAST_SUBSCRIPTIONS:tasks.append(asyncio.create_task(run_socket('hl_fast',HL_URL,HL_FAST_SUBSCRIPTIONS)))
 if HL_AGG_SUBSCRIPTIONS:tasks.append(asyncio.create_task(run_socket('hl_agg',HL_URL,HL_AGG_SUBSCRIPTIONS)))
 try:await monitor(tasks)
 finally:
  STOP.set()
  for t in tasks:t.cancel()
  await asyncio.gather(*tasks,return_exceptions=True)
  event('process_stopped',reason=STOP_REASON)
  for s in list(ACTIVE):close_chunk(s)
  atomic_status(status())

if __name__=='__main__':
 import argparse
 parser=argparse.ArgumentParser();parser.add_argument('--output',type=Path,default=DEFAULT_ROOT/'data/live')
 configure(parser.parse_args().output)
 asyncio.run(main())
