"""Merge own recorded market events with explicitly tagged external HL summaries."""
import argparse, hashlib, heapq, importlib.util, json, sqlite3, sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2]
spec=importlib.util.spec_from_file_location('tape_export',Path(__file__).with_name('tape-export.py'));adapter=importlib.util.module_from_spec(spec);spec.loader.exec_module(adapter)
spec2=importlib.util.spec_from_file_location('quote_coalesce',Path(__file__).with_name('quote-coalesce.py'));coalescer=importlib.util.module_from_spec(spec2);spec2.loader.exec_module(coalescer)
def main():
 p=argparse.ArgumentParser();p.add_argument('--from-ms',type=int,required=True);p.add_argument('--to-ms',type=int,required=True);p.add_argument('--out',type=Path,required=True);p.add_argument('--coalesce-binance-ms',type=int,choices=[0,100],default=0);a=p.parse_args()
 c=sqlite3.connect((ROOT/'data/unified/market.sqlite3').as_uri()+'?mode=ro',uri=True,timeout=5);c.row_factory=sqlite3.Row;c.execute('PRAGMA query_only=ON');c.execute('PRAGMA cache_size=-8192')
 sources=c.execute("SELECT id,path,sha256,venue_hint,min_ms,max_ms,last_recv_ms FROM sources WHERE role='live' AND dataset IN ('observer','public_supplement') AND venue_hint IN ('hl','binance_futures','binance_spot') AND min_ms<=? AND last_recv_ms>=? ORDER BY id",(a.to_ms,a.from_ms)).fetchall()
 stats={'fromMs':a.from_ms,'toMs':a.to_ms,'sources':[dict(s) for s in sources],'counts':{},'coverage':{},'activeCoverage':{},'rawRows':0,'events':0};h=hashlib.sha256();heap=[]
 def push(cur,kind,venue):
  r=cur.fetchone()
  if r is not None:heapq.heappush(heap,(r['recv_ms'],r['id'],kind,venue,r,cur))
 for s in sources:
  lo,hi=s['id']<<40,((s['id']+1)<<40)-1
  cur=c.execute("SELECT id,recv_ms,raw_z FROM records WHERE id BETWEEN ? AND ? AND recv_ms>=? AND recv_ms<=? AND symbol='BTC' AND kind IN ('bbo','l2Book','bookTicker','depthUpdate') ORDER BY id",(lo,hi,a.from_ms,a.to_ms));push(cur,'market',s['venue_hint'])
 cur=c.execute('SELECT record_id id,at_ms recv_ms,* FROM strategy_hl_book_summaries WHERE at_ms>=? AND at_ms<=? ORDER BY at_ms,record_id',(a.from_ms,a.to_ms));push(cur,'summary','hl')
 last=-1;bucket=None;pending=[];stats['rawValidEvents']=0
 stats['coalescingPolicy']='BF/BS quotes: latest source-monotonic arrival before each100ms decision; HL/all books/summaries retained' if a.coalesce_binance_ms else None
 def write_event(t,rid,e):
  stats['events']+=1;e['sequence']=stats['events'];e['payload_sha256']=hashlib.sha256(json.dumps(e['payload'],separators=(',',':'),ensure_ascii=False).encode()).hexdigest()
  key=e['venue']+':'+e['payload']['frameKind'];stats['counts'][key]=stats['counts'].get(key,0)+1
  k=stats['coverage'].setdefault(key,{'firstMs':t,'lastMs':t,'maxGapMs':0,'events':0});k['events']+=1;k['maxGapMs']=max(k['maxGapMs'],t-k['lastMs']);k['lastMs']=t
  line=json.dumps(e,separators=(',',':'),ensure_ascii=False)+'\n';h.update(line.encode());sys.stdout.write(line)
 def flush():
  for t,rid,e in coalescer.bucket_events(pending):write_event(t,rid,e)
  pending.clear()
 while heap:
  t,rid,kind,venue,r,cur=heapq.heappop(heap);stats['rawRows']+=1;push(cur,kind,venue)
  if kind=='summary':
   e={'kind':'book_summary','symbol':'BTC','venue':'hl','source_ms':None,'payload':{'frameKind':'book_summary','mid':r['mid'],'imbalancePct':r['imbalance_pct'],'bidNearUsd':r['bid_near_usd'],'askNearUsd':r['ask_near_usd'],'wallUsd':r['wall_usd'],'wallSide':r['wall_side'],'wallDistanceUsd':r['wall_distance_usd']},'sourceId':r['source_id'],'clockPolicy':'External snapshot epoch treated as availability for scenario only; original receive/source clocks unavailable'}
  else:
   e=adapter.event(r,venue)
   if not e:continue
  if t<last:raise ValueError('Merged receive regression')
  last=t;stats['rawValidEvents']+=1;e.update(schemaVersion=1,recv_ms=t,sourceRecordId=str(rid),provenance='External HL derived summary' if kind=='summary' else 'Own recorded live capture')
  if a.coalesce_binance_ms:
   b=int(t//100)
   if bucket is not None and b!=bucket:flush()
   bucket=b;pending.append((t,rid,e))
  else:write_event(t,rid,e)
 if pending:flush()
 stats['discardedIntermediateBinanceQuotes']=stats['rawValidEvents']-stats['events']
 stats['sha256']=h.hexdigest();a.out.write_text(json.dumps(stats,ensure_ascii=False,indent=2));sys.stderr.write(json.dumps({k:v for k,v in stats.items() if k not in ['sources']})+'\n')
if __name__=='__main__':main()
