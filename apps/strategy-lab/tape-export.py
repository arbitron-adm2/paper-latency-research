"""Read-only streaming input adapter. No historical/selected trade windows mixed in."""
import argparse,sqlite3,zlib,json,heapq,hashlib,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2]
def event(row,venue):
 try:o=json.loads(zlib.decompress(row['raw_z']));data=o.get('data',{});p=None;source=None;symbol=None
 except (ValueError,zlib.error,AttributeError):return None
 if venue=='paper':
  pubs=list(data.get('subscribe',{}).get('publications',[]))
  if data.get('push',{}).get('pub'):pubs.append(data['push']['pub'])
  if not pubs:pubs=[{'data':data}]
  for pub in pubs:
   d=pub.get('data');snap=d.get('snapshot',d) if isinstance(d,dict)else None
   if isinstance(snap,dict)and('totalPendingGas'in snap or 'chain'in snap):p=d;source=snap.get('ts')
  if p is None:return None
  return {'kind':'pressure','symbol':None,'venue':'paper','source_ms':source,'payload':p}
 if venue=='hl':
  d=data.get('data',{});symbol=d.get('coin');source=d.get('time');kind=data.get('channel')
  if kind=='bbo'and len(d.get('bbo',[]))==2:
   b,a=d['bbo'];p={'bid':str(b['px']),'ask':str(a['px']),'bidSize':str(b.get('sz','')),'askSize':str(a.get('sz',''))}
  elif kind=='l2Book'and len(d.get('levels',[]))==2 and all(d['levels']):
   b,a=d['levels'];p={'bid':str(b[0]['px']),'ask':str(a[0]['px']),'bids':[[str(x['px']),str(x['sz'])]for x in b[:100]],'asks':[[str(x['px']),str(x['sz'])]for x in a[:100]]}
 else:
  d=data.get('data',data);stream=data.get('stream','');s=d.get('s',stream.split('@')[0].upper());symbol=s[:-4]if s.endswith('USDT')else None;source=d.get('E',d.get('T'))
  if isinstance(d.get('b'),str)and isinstance(d.get('a'),str):p={'bid':d['b'],'ask':d['a'],'bidSize':str(d.get('B','')),'askSize':str(d.get('A',''))}
  else:
   b,a=d.get('b',d.get('bids',[])),d.get('a',d.get('asks',[]))
   if isinstance(b,list)and isinstance(a,list)and b and a:p={'bid':str(b[0][0]),'ask':str(a[0][0]),'bids':[[str(x[0]),str(x[1])]for x in b[:100]],'asks':[[str(x[0]),str(x[1])]for x in a[:100]]}
 if symbol not in('BTC','ETH')or not p:return None
 p.update(frameKind='book'if'bids'in p else'quote',instrumentId=0 if symbol=='BTC'else 1,priceScale='10'if symbol=='BTC'else'100',sourceTimestampKnown=source is not None)
 return {'kind':'market','symbol':symbol,'venue':{'binance_futures':'bf','binance_spot':'bs'}.get(venue,venue),'source_ms':source,'payload':p}
def export(db,start,end,mode):
 c=sqlite3.connect(db.as_uri()+'?mode=ro',uri=True);c.row_factory=sqlite3.Row;c.execute('PRAGMA query_only=ON');n=0
 if mode=='canonical':
  rows=c.execute('SELECT boot_id,sha256,data_z FROM strategy_tape_chunks WHERE last_recv_ms>=? AND first_recv_ms<=? ORDER BY id',(start,end));last={}
  for chunk in rows:
   raw=zlib.decompress(chunk['data_z'])
   if hashlib.sha256(raw).hexdigest()!=chunk['sha256']:raise ValueError('TAPE_CHECKSUM')
   for e in json.loads(raw):
    if start<=e['recv_ms']<=end and e['sequence']>last.get(chunk['boot_id'],0):
     last[chunk['boot_id']]=e['sequence'];e['captureBootId']=chunk['boot_id'];print(json.dumps(e,separators=(',',':')));n+=1
 else:
  sources=c.execute("SELECT id,venue_hint FROM sources WHERE role='live' AND venue_hint IN ('hl','hl_agg','binance_futures','binance_spot','paper') AND last_recv_ms>=?",(start,)).fetchall();heap=[];cursors=[]
  def advance(cur,venue):
   r=cur.fetchone()
   if r:heapq.heappush(heap,(r['recv_ms'],r['id'],venue,r,cur))
  for s in sources:
   lo,hi=s['id']<<40,((s['id']+1)<<40)-1
   first=c.execute('SELECT recv_ms FROM records WHERE id BETWEEN ? AND ? ORDER BY id LIMIT 1',(lo,hi)).fetchone()
   if first and first[0] is not None and first[0]>end:continue
   cur=c.execute("SELECT id,recv_ms,raw_z FROM records WHERE id BETWEEN ? AND ? AND recv_ms>=? AND recv_ms<=? AND kind IN ('bbo','l2Book','bookTicker','depthUpdate','pressure','trading') ORDER BY id",(lo,hi,start,end));cursors.append(cur);advance(cur,s['venue_hint'])
  while heap:
   t,rid,venue,row,cur=heapq.heappop(heap);e=event(row,'hl'if venue=='hl_agg'else venue);advance(cur,venue)
   if not e:continue
   n+=1;e.update(schemaVersion=1,sequence=n,recv_ms=t,sourceRecordId=str(rid),captureVenue=venue,provenance='Unified DB continuous live source; receive-clock replay; original raw_z')
   e['payload_sha256']=hashlib.sha256(json.dumps(e['payload'],separators=(',',':'),ensure_ascii=False).encode()).hexdigest();print(json.dumps(e,separators=(',',':'),ensure_ascii=False))
  for cur in cursors:cur.close()
 c.close();sys.stderr.write(json.dumps({'mode':mode,'events':n,'fromMs':start,'toMs':end})+'\n')
if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--from-ms',type=int,required=True);p.add_argument('--to-ms',type=int,required=True);p.add_argument('--mode',choices=['canonical','continuous'],default='canonical');a=p.parse_args();export(ROOT/'data/unified/market.sqlite3',a.from_ms,a.to_ms,a.mode)
