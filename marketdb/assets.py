"""Lossless file storage in the same SQLite database; no source code execution."""
import hashlib
import base64
import io
import json
import mimetypes
import os
import sqlite3
import time
import zlib
from pathlib import Path

ASSET_SCHEMA = '''
CREATE TABLE IF NOT EXISTS assets(sha256 TEXT PRIMARY KEY,bytes INTEGER NOT NULL,chunks INTEGER NOT NULL,
 mime TEXT,complete INTEGER NOT NULL DEFAULT 0,created_ms REAL NOT NULL);
CREATE TABLE IF NOT EXISTS asset_chunks(sha256 TEXT NOT NULL,part INTEGER NOT NULL,raw_bytes INTEGER NOT NULL,
 data BLOB NOT NULL,PRIMARY KEY(sha256,part)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS files(path TEXT PRIMARY KEY,sha256 TEXT NOT NULL,category TEXT NOT NULL,
 source_path TEXT,mtime_ns INTEGER,mode INTEGER,bytes INTEGER,updated_ms REAL);
CREATE TABLE IF NOT EXISTS file_versions(path TEXT NOT NULL,sha256 TEXT NOT NULL,imported_ms REAL NOT NULL,
 PRIMARY KEY(path,sha256)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS file_links(path TEXT PRIMARY KEY,target TEXT NOT NULL,category TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS native_catalog(namespace TEXT NOT NULL,source_id TEXT NOT NULL,source_path TEXT NOT NULL,
 source_table TEXT NOT NULL,target_table TEXT NOT NULL,source_schema TEXT,expected_rows INTEGER,
 imported_rows INTEGER DEFAULT 0,complete INTEGER DEFAULT 0,synced_ms REAL,
 PRIMARY KEY(namespace,source_id,source_table)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS research_runs(name TEXT PRIMARY KEY,assignment_id TEXT,status TEXT,
 result_path TEXT,report_path TEXT,updated_ms REAL);
CREATE TABLE IF NOT EXISTS native_checks(namespace TEXT,source_id TEXT,source_table TEXT,
 expected_sha256 TEXT,actual_sha256 TEXT,rows INTEGER,checked_ms REAL,
 PRIMARY KEY(namespace,source_id,source_table)) WITHOUT ROWID;
'''


class AssetReader(io.RawIOBase):
    def __init__(self, c, sha):
        self.rows = iter(c.execute('SELECT data,raw_bytes FROM asset_chunks WHERE sha256=? ORDER BY part',(sha,)))
        self.buf = b''
    def readable(self): return True
    def readinto(self, target):
        n = 0
        while n < len(target):
            if not self.buf:
                try: data,size = next(self.rows)
                except StopIteration: break
                self.buf = zlib.decompress(data)
                if len(self.buf) != size: raise RuntimeError('Corrupt asset chunk')
            k = min(len(target)-n,len(self.buf)); target[n:n+k] = self.buf[:k]
            self.buf = self.buf[k:]; n += k
        return n


def init_assets(c):
    c.executescript(ASSET_SCHEMA);c.commit()


def discard_incomplete(c, sha):
    """Discard only unreferenced failed uploads; never a completed archive asset."""
    r=c.execute('SELECT complete FROM assets WHERE sha256=?',(sha,)).fetchone()
    if not r or r[0]:return False
    if c.execute('SELECT 1 FROM files WHERE sha256=? LIMIT 1',(sha,)).fetchone():return False
    if c.execute('SELECT 1 FROM file_versions WHERE sha256=? LIMIT 1',(sha,)).fetchone():return False
    with c:
        c.execute('DELETE FROM asset_chunks WHERE sha256=?',(sha,))
        c.execute('DELETE FROM assets WHERE sha256=? AND complete=0',(sha,))
    return True


def store_file(c, logical, path, category):
    path=Path(path)
    if path.is_symlink():
        with c: c.execute('INSERT OR REPLACE INTO file_links VALUES(?,?,?)',(logical,os.readlink(path),category))
        return False
    st=path.stat()
    prior=c.execute('SELECT * FROM files WHERE path=?',(logical,)).fetchone()
    if prior and prior['mtime_ns']==st.st_mtime_ns and prior['bytes']==st.st_size:
        return False
    h=hashlib.sha256()
    with path.open('rb') as f:
        for b in iter(lambda:f.read(4*1024*1024),b''):h.update(b)
    after_hash=path.stat()
    if after_hash.st_mtime_ns!=st.st_mtime_ns or after_hash.st_size!=st.st_size:
        raise RuntimeError('Source changed during asset snapshot: '+str(path))
    sha=h.hexdigest();a=c.execute('SELECT complete FROM assets WHERE sha256=?',(sha,)).fetchone()
    if not a or not a[0]:
        try:
            with c:
                c.execute('INSERT OR IGNORE INTO assets VALUES(?,?,0,?,0,?)',(sha,st.st_size,mimetypes.guess_type(path.name)[0],time.time()*1000))
                c.execute('DELETE FROM asset_chunks WHERE sha256=?',(sha,))
            count=0;check=hashlib.sha256()
            with path.open('rb') as f:
                for b in iter(lambda:f.read(4*1024*1024),b''):
                    packed=zlib.compress(b,3);check.update(b)
                    with c:c.execute('INSERT INTO asset_chunks VALUES(?,?,?,?)',(sha,count,len(b),packed))
                    count+=1
            if check.hexdigest()!=sha or path.stat().st_mtime_ns!=st.st_mtime_ns or path.stat().st_size!=st.st_size:
                raise RuntimeError('Source changed during asset snapshot: '+str(path))
            # Decompress stored chunks and verify original bytes before marking complete.
            verify=hashlib.sha256()
            with io.BufferedReader(AssetReader(c,sha)) as stream:
                for b in iter(lambda:stream.read(4*1024*1024),b''):verify.update(b)
            if verify.hexdigest()!=sha:raise RuntimeError('Stored asset SHA256 mismatch')
            with c:c.execute('UPDATE assets SET complete=1,chunks=? WHERE sha256=?',(count,sha))
        except Exception:
            discard_incomplete(c,sha)
            raise
    now=time.time()*1000
    with c:
        c.execute('INSERT OR REPLACE INTO files VALUES(?,?,?,?,?,?,?,?)',
                  (logical,sha,category,str(path),st.st_mtime_ns,st.st_mode&0o777,st.st_size,now))
        c.execute('INSERT OR IGNORE INTO file_versions VALUES(?,?,?)',(logical,sha,now))
    return True


def restore_file(c, logical, target):
    r=c.execute('SELECT f.*,a.complete FROM files f JOIN assets a USING(sha256) WHERE path=?',(logical,)).fetchone()
    if not r or not r['complete']:raise FileNotFoundError(logical)
    target=Path(target)
    if target.exists():raise FileExistsError('Preserving existing destination: '+str(target))
    target.parent.mkdir(parents=True,exist_ok=True)
    part=target.with_name(target.name+'.restoring')
    h=hashlib.sha256()
    with io.BufferedReader(AssetReader(c,r['sha256'])) as stream,part.open('xb') as f:
        for b in iter(lambda:stream.read(4*1024*1024),b''):h.update(b);f.write(b)
    if h.hexdigest()!=r['sha256']:raise RuntimeError('Restore checksum failed')
    os.chmod(part,0o600);part.rename(target)
    return {'path':str(target),'sha256':r['sha256'],'bytes':r['bytes']}


def qi(s):return '"'+s.replace('"','""')+'"'

def row_bytes(row):
    return (json.dumps(list(row),ensure_ascii=False,separators=(',',':'),
            default=lambda x:{'__blob_base64':base64.b64encode(x).decode()})+'\n').encode()


def import_sqlite(c,path,namespace,live=False):
    """Original table values become queryable tables in the one research DB."""
    path=Path(path)
    if not path.is_file():return []
    if live:
        source_id=hashlib.sha256(str(path.resolve()).encode()).hexdigest()
    else:
        stat=path.stat()
        cached=c.execute('SELECT f.sha256,f.mtime_ns,f.bytes FROM files f JOIN assets a USING(sha256) WHERE f.source_path=? AND a.complete=1 LIMIT 1',(str(path.resolve()),)).fetchone()
        if cached and cached['mtime_ns']==stat.st_mtime_ns and cached['bytes']==stat.st_size:
            source_id=cached['sha256']
        else:
            h=hashlib.sha256()
            with path.open('rb') as f:
                for b in iter(lambda:f.read(4*1024*1024),b''):h.update(b)
            source_id=h.hexdigest()
    src=sqlite3.connect(path.resolve().as_uri()+('?mode=ro' if live else '?mode=ro&immutable=1'),uri=True,timeout=10)
    src.execute('BEGIN')
    tables=src.execute("SELECT name,sql FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'").fetchall()
    output=[]
    for name,schema in tables:
        # Imported archive is sanitized; operational authentication tables are never projected.
        if live and not any(k in name for k in ['trade','position','queue','config','profile','tune','lab','test','hedge','oi_block','execution','audit']):continue
        if any(k in name for k in ['api_keys','user_sessions','access_codes']):
            # Preserve empty schemas from the sanitized archive; do not copy auth material.
            n=src.execute('SELECT COUNT(*) FROM '+qi(name)).fetchone()[0]
            if n:continue
        expected=src.execute('SELECT COUNT(*) FROM '+qi(name)).fetchone()[0]
        prev=c.execute('SELECT * FROM native_catalog WHERE namespace=? AND source_id=? AND source_table=?',(namespace,source_id,name)).fetchone()
        if prev and prev['complete'] and not live:continue
        cols=src.execute('PRAGMA table_info('+qi(name)+')').fetchall()
        names=[x[1] for x in cols];target=namespace+'__'+name
        # Preserve names and values; use SQLite affinity types, never execute source CREATE statements.
        declarations=[]
        for x in cols:
            kind=(x[2] or '').upper();aff='INTEGER' if 'INT' in kind else 'REAL' if any(k in kind for k in ['REAL','FLOA','DOUB']) else 'TEXT' if any(k in kind for k in ['TEXT','CHAR','CLOB']) else 'BLOB'
            declarations.append(qi(x[1])+' '+aff)
        c.execute('CREATE TABLE IF NOT EXISTS '+qi(target)+'(__source_id TEXT NOT NULL,__source_row INTEGER NOT NULL,'+','.join(declarations)+',PRIMARY KEY(__source_id,__source_row)) WITHOUT ROWID')
        present={r[1] for r in c.execute('PRAGMA table_info('+qi(target)+')')}
        for col,decl in zip(names,declarations):
            if col not in present:c.execute('ALTER TABLE '+qi(target)+' ADD COLUMN '+decl)
        c.execute('INSERT OR REPLACE INTO native_catalog VALUES(?,?,?,?,?,?,?,0,0,?)',
                  (namespace,source_id,str(path),name,target,schema,expected,time.time()*1000));c.commit()
        if live:
            with c:c.execute('DELETE FROM '+qi(target)+' WHERE __source_id=?',(source_id,))
        else:
            with c:c.execute('DELETE FROM '+qi(target)+' WHERE __source_id=?',(source_id,))
        n=0;rows=src.execute('SELECT * FROM '+qi(name));expected_hash=hashlib.sha256()
        while True:
            batch=rows.fetchmany(2000)
            if not batch:break
            data=[]
            for row in batch:
                n+=1
                values=list(row)
                for i,col in enumerate(names):
                    if any(k in col.lower().replace('_','') for k in ['privatekey','mnemonic','apisecret','accesstoken','refreshtoken','sessiontoken','password','passphrase']):values[i]=None
                data.append((source_id,n,*values))
                expected_hash.update(row_bytes(values))
            with c:
                c.executemany('INSERT INTO '+qi(target)+'(__source_id,__source_row,'+','.join(map(qi,names))+') VALUES('+','.join('?' for _ in range(len(names)+2))+')',data)
                c.execute('UPDATE native_catalog SET imported_rows=? WHERE namespace=? AND source_id=? AND source_table=?',(n,namespace,source_id,name))
        if n!=expected:raise RuntimeError('Source table count changed inside snapshot')
        actual=c.execute('SELECT COUNT(*) FROM '+qi(target)+' WHERE __source_id=?',(source_id,)).fetchone()[0]
        if actual!=expected:raise RuntimeError('Native table copy count mismatch')
        copied_hash=hashlib.sha256()
        for row in c.execute('SELECT '+','.join(map(qi,names))+' FROM '+qi(target)+' WHERE __source_id=? ORDER BY __source_row',(source_id,)):
            copied_hash.update(row_bytes(row))
        if expected_hash.hexdigest()!=copied_hash.hexdigest():raise RuntimeError('Native table values or types changed: '+target)
        with c:
            c.execute('UPDATE native_catalog SET complete=1,synced_ms=? WHERE namespace=? AND source_id=? AND source_table=?',(time.time()*1000,namespace,source_id,name))
            c.execute('INSERT OR REPLACE INTO native_checks VALUES(?,?,?,?,?,?,?)',(namespace,source_id,name,expected_hash.hexdigest(),copied_hash.hexdigest(),n,time.time()*1000))
        output.append({'source':name,'target':target,'rows':n})
    src.close()
    return output
