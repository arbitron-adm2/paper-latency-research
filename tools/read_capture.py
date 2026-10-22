"""Stream raw capture rows, including a fixed prefix of active gzip files."""
import argparse,gzip,json,zlib
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
def rows(path,active=False):
 if active:
  remaining=path.stat().st_size;dec=zlib.decompressobj(16+zlib.MAX_WBITS);buf=b''
  with path.open('rb') as f:
   while remaining:
    part=f.read(min(1024*1024,remaining))
    if not part:break
    remaining-=len(part);buf+=dec.decompress(part);parts=buf.split(b'\n');buf=parts.pop()
    for line in parts:
     if line:yield json.loads(line)
 else:
  opener=gzip.open if path.suffix=='.gz' else open
  with opener(path,'rt') as f:
   for line in f:
    if line.strip():yield json.loads(line)
def main():
 a=argparse.ArgumentParser();a.add_argument('--dataset',choices=['live','window1','window2','window3'],default='live');a.add_argument('--source',choices=['hl','hl_fast','hl_agg','binance','paper','control'],default='hl');a.add_argument('--since-ms',type=float,default=0);a.add_argument('--until-ms',type=float,default=float('inf'));a.add_argument('--limit',type=int,default=5);args=a.parse_args()
 files=[]
 if args.dataset=='live':
  p=ROOT/'data'/'live';status=json.loads((p/'status.json').read_text())
  for line in (p/'manifest.jsonl').read_text().splitlines():
   m=json.loads(line)
   if m['source']==args.source and m['last_recv_ms']>=args.since_ms and m['first_recv_ms']<=args.until_ms:files.append((m['first_recv_ms'],p/m['path'],False))
  for m in status.get('active_chunks',[]):
   if m['source']==args.source and m['last_recv_ms']>=args.since_ms and m['first_recv_ms']<=args.until_ms:files.append((m['first_recv_ms'],p/m['path'],True))
 else:files=[(0,ROOT/'data'/'legacy'/args.dataset/(args.source+'.jsonl'),False)]
 n=0;seen=set()
 for _,p,active in sorted(files):
  if p in seen:continue
  seen.add(p)
  for r in rows(p,active):
   if args.since_ms<=r['recv_ms']<=args.until_ms:
    print(json.dumps(r,separators=(',',':')));n+=1
    if args.limit and n>=args.limit:return
if __name__=='__main__':main()
