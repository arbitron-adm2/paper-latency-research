import json,time,datetime
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
def main():
 p=ROOT/'data'/'live';s=json.loads((p/'status.json').read_text());counts={}
 for line in (p/'manifest.jsonl').read_text().splitlines():
  r=json.loads(line);counts[r['source']]=counts.get(r['source'],0)+r['rows']
 for r in s.get('active_chunks',[]):counts[r['source']]=counts.get(r['source'],0)+r['rows']
 age=time.time()-s['now_ms']/1000
 out={'checked_at_moscow':datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=3))).isoformat(),'status_age_s':round(age,2),'health':s['health'] if age<15 else 'STALE_STATUS','pid':s['pid'],'counts_all_runs':counts,'data_rows_excluding_control':sum(v for k,v in counts.items() if k!='control'),'error_counts':s['error_counts'],'connected':s['connected'],'capture_directory':str(p.resolve()),'deadline':s['measurement_end_ms']}
 print(json.dumps(out,ensure_ascii=False,indent=2))
if __name__=='__main__':main()
