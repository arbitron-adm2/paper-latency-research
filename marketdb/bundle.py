"""Catalog all authorized research files, original tables and verified cold archive."""
import argparse
import datetime as dt
import fcntl
import hashlib
import io
import json
import os
import signal
import tarfile
import time
from pathlib import Path
from .core import DEFAULT_ROOT, connect
from .assets import init_assets, store_file, import_sqlite, AssetReader, restore_file

SKIP_DIRS={'.git','node_modules','__pycache__','.pytest_cache','.venv','venv','.runtime','wallets',
           'persistent-shadow-state','basket-sim-20261020','copy-6ec3-shadow-20261021'}
SKIP_NAMES={'.git','wallet.json','credentials.json','cookies.json','storage_state.json','session-key.json','session_key.json'}


def walk(base):
    for directory,dirs,files in os.walk(base,followlinks=False):
        dirs[:]=[x for x in dirs if x not in SKIP_DIRS]
        for name in list(dirs):
            p=Path(directory)/name
            if p.is_symlink():yield p;dirs.remove(name)
        for name in files:
            p=Path(directory)/name
            if name in SKIP_NAMES or p.suffix in {'.key','.pem','.session','.pyc'} or name.endswith(('.lock','.part','.new')):continue
            if any(name.endswith(ext+sidecar) for ext in ('.sqlite','.sqlite3','.db') for sidecar in ('-wal','-shm','-journal')):continue
            # Mutable databases are owned by their services, not whole-file versioned assets.
            # Closed immutable SQLite inputs remain eligible for the lossless archive.
            if p.suffix in {'.sqlite','.sqlite3','.db'} and any(Path(str(p)+sidecar).exists() for sidecar in ('-wal','-shm','-journal')):continue
            if name.startswith('.env'):continue
            yield p


def candidates(root):
    for name in ['incoming/vlad','evidence','reports','research','clients','sources','data/legacy','data/capture_snapshot','data/capture_archive','docs','tools','tests','apps/vchera2','operations']:
        base=root/name
        if not base.exists():continue
        for path in walk(base):
            if path.name in ['watch_status.json','watch_pipeline.log','writer.lock','watch.lock','fetch.lock']:continue
            yield 'project/'+str(path.relative_to(root)),path,'vlad_originals' if name=='incoming/vlad' else 'project_research'
    for name in ['README.md','AGENTS.md','.gitignore','project.json']:
        if (root/name).is_file():yield 'project/'+name,root/name,'project_context'
    for name in ['README.md','acceptance.json','full_catalog_acceptance.json','archive_latest.json']:
        path=root/'data/unified'/name
        if path.is_file():yield 'project/data/unified/'+name,path,'database_validation'
    for path in (root/'archives').glob('*.manifest.json'):
        yield 'project/archives/'+path.name,path,'archive_validation'
    for path in walk(root/'.worktree/unified-market-db'):
        yield 'database_code/'+str(path.relative_to(root/'.worktree/unified-market-db')),path,'database_code'
    for folder,prefix in [
        (root/'reports','research_workers'),
        (root/'reports','launch_implementation')]:
        if folder.exists():
            for path in walk(folder):yield prefix+'/'+str(path.relative_to(folder)),path,'research_results'
    # Closed raw chunks only: active files remain a bounded append journal, never snapshotted mid-write.
    for base,prefix in [(root/'data/live','observer_closed'),(root/'data/unified/supplemental','supplemental_closed')]:
        f=base/'manifest.jsonl'
        if not f.exists():continue
        for line in f.read_text().splitlines():
            try:r=json.loads(line)
            except json.JSONDecodeError:continue
            path=base/r['path']
            if path.is_file():yield prefix+'/'+path.name,path,'completed_public_capture'


def sync(root):
    c=connect(root/'data/unified/market.sqlite3');init_assets(c)
    out={'started_ms':time.time()*1000,'new_file_versions':0,'volatile_files_deferred':[]}
    disk=os.statvfs(root/'data/unified')
    if disk.f_bavail*disk.f_frsize < 12*1024**3:
        out.update(status='DISK_RESERVE_PAUSED',finished_ms=time.time()*1000)
        (root/'data/unified/catalog_status.json').write_text(json.dumps(out,indent=2))
        c.close();return out
    for logical,path,category in candidates(root):
        try:
            changed=store_file(c,logical,path,category);out['new_file_versions']+=bool(changed)
        except FileNotFoundError:out['volatile_files_deferred'].append(logical);continue
        except RuntimeError as e:
            if 'Source changed during asset snapshot' in str(e):out['volatile_files_deferred'].append(logical);continue
            raise
        if out['new_file_versions'] and out['new_file_versions']%100==0 and changed:
            print(json.dumps({'event':'asset_progress','new_versions':out['new_file_versions'],'last_file':logical}),flush=True)
    path=root/'sources/vlad_trade_journal/data/journal.db'
    out['vlad_tables_imported']=import_sqlite(c,path,'vlad')
    out['operational_tables_projected']=import_sqlite(c,root/'apps/vchera2/.runtime/vchera2.sqlite','bot',live=True)
    for role in ['strategy_logic','trades_latency','chain_validation','hedge_replay']:
        rel='research_workers/'+role+'/result.json'
        f=c.execute('SELECT source_path FROM files WHERE path=?',(rel,)).fetchone()
        if f:
            d=json.loads(Path(f[0]).read_text())
            with c:c.execute('INSERT OR REPLACE INTO research_runs VALUES(?,?,?,?,?,?)',
                (role,d.get('assignment_id'),d.get('status'),rel,'research_workers/'+role+'/report.md',time.time()*1000))
    out.update(finished_ms=time.time()*1000,files=c.execute('SELECT COUNT(*) FROM files').fetchone()[0],
               unique_assets=c.execute('SELECT COUNT(*) FROM assets WHERE complete=1').fetchone()[0],
               native_tables=c.execute('SELECT COUNT(*) FROM native_catalog WHERE complete=1').fetchone()[0],
               native_rows=c.execute('SELECT SUM(imported_rows) FROM native_catalog WHERE complete=1').fetchone()[0])
    p=root/'data/unified/catalog_status.json';q=p.with_suffix('.new');q.write_text(json.dumps(out,ensure_ascii=False,indent=2));q.replace(p)
    print(json.dumps({k:v for k,v in out.items() if k not in ['vlad_tables_imported','operational_tables_projected']},ensure_ascii=False),flush=True)
    # Release a WAL high-water allocation without waiting on collectors/readers.
    wal=Path(str(root/'data/unified/market.sqlite3')+'-wal')
    if wal.exists() and wal.stat().st_size>256*1024**2:
        c.execute('PRAGMA busy_timeout=0')
        c.execute('PRAGMA wal_checkpoint(TRUNCATE)').fetchone()
    c.close();return out


def archive(root):
    c=connect(root/'data/unified/market.sqlite3',readonly=True)
    entries=[dict(r) for r in c.execute('SELECT f.*,a.complete FROM files f JOIN assets a USING(sha256) ORDER BY path')]
    links=[dict(r) for r in c.execute('SELECT * FROM file_links ORDER BY path')]
    if any(not r['complete'] for r in entries):raise RuntimeError('Incomplete archived asset')
    stamp=dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    folder=root/'archives';folder.mkdir(exist_ok=True)
    dest=folder/('research-originals-'+stamp+'.tar.gz');part=dest.with_suffix(dest.suffix+'.part')
    manifest={'created_utc':dt.datetime.now(dt.timezone.utc).isoformat(),'database':str(root/'data/unified/market.sqlite3'),
              'scope':'All catalogued Vlad originals/extracted files, historical captures, source code, tests and research results. Operational credentials and active database/WAL/runtime excluded.',
              'files':entries,'links':links,'logical_files':len(entries),'logical_bytes':sum(r['bytes'] for r in entries)}
    with tarfile.open(part,'w:gz',compresslevel=1) as tar:
        for i,r in enumerate(entries):
            info=tarfile.TarInfo(r['path']);info.size=r['bytes'];info.mode=r['mode'] or 0o600;info.mtime=int(r['updated_ms']/1000)
            with io.BufferedReader(AssetReader(c,r['sha256'])) as stream:tar.addfile(info,stream)
            if i%200==0:print(json.dumps({'event':'archive_write','files':i}),flush=True)
        for link in links:
            info=tarfile.TarInfo(link['path']);info.type=tarfile.SYMTYPE;info.linkname=link['target'];tar.addfile(info)
        raw=json.dumps(manifest,ensure_ascii=False,indent=2).encode();info=tarfile.TarInfo('ARCHIVE-MANIFEST.json');info.size=len(raw);tar.addfile(info,io.BytesIO(raw))
    expected={r['path']:r for r in entries};seen=set()
    with tarfile.open(part,'r:gz') as tar:
        for item in tar:
            if not item.isfile() or item.name=='ARCHIVE-MANIFEST.json':continue
            row=expected[item.name];h=hashlib.sha256();n=0
            with tar.extractfile(item) as f:
                for b in iter(lambda:f.read(4*1024*1024),b''):h.update(b);n+=len(b)
            if h.hexdigest()!=row['sha256'] or n!=row['bytes']:raise RuntimeError('Archive entry mismatch: '+item.name)
            seen.add(item.name)
            if len(seen)%200==0:print(json.dumps({'event':'archive_verified','files':len(seen)}),flush=True)
    if seen!=set(expected):raise RuntimeError('Archive missing members')
    part.rename(dest);h=hashlib.sha256()
    with dest.open('rb') as f:
        for b in iter(lambda:f.read(4*1024*1024),b''):h.update(b)
    report={'archive':str(dest),'archive_bytes':dest.stat().st_size,'archive_sha256':h.hexdigest(),
            'files_verified':len(seen),'logical_bytes':manifest['logical_bytes'],'complete':True,
            'source_deletion':False,'active_runtime_untouched':True,'primary_database':str(root/'data/unified/market.sqlite3')}
    (dest.with_suffix(dest.suffix+'.manifest.json')).write_text(json.dumps(report,ensure_ascii=False,indent=2))
    (root/'data/unified/archive_latest.json').write_text(json.dumps(report,ensure_ascii=False,indent=2))
    print(json.dumps(report,ensure_ascii=False,indent=2),flush=True);c.close();return report


def main():
    p=argparse.ArgumentParser();p.add_argument('command',choices=['sync','watch','archive','restore','files']);p.add_argument('--project-root',type=Path,default=DEFAULT_ROOT);p.add_argument('--interval',type=int,default=60);p.add_argument('--path');p.add_argument('--output',type=Path)
    a=p.parse_args();root=a.project_root
    if a.command in ['sync','watch']:
        lock=(root/'data/unified/catalog.lock').open('a');fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        if a.command=='sync':sync(root);return
        stop=False
        def stopping(*_):
            nonlocal stop
            stop=True
        for sig in [signal.SIGTERM,signal.SIGINT]:signal.signal(sig,stopping)
        while not stop:
            sync(root)
            for _ in range(a.interval):
                if stop:break
                time.sleep(1)
    elif a.command=='archive':archive(root)
    else:
        c=connect(root/'data/unified/market.sqlite3',readonly=True)
        if a.command=='restore':
            if not a.path or not a.output:p.error('--path and --output required')
            print(json.dumps(restore_file(c,a.path,a.output)))
        else:
            for r in c.execute('SELECT path,sha256,category,bytes FROM files WHERE path LIKE ? ORDER BY path',(a.path or '%',)):print(json.dumps(dict(r),ensure_ascii=False))


if __name__=='__main__':main()
