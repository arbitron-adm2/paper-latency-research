"""Finite untouched forward replay of a declarative candidate; no orders or WS."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import time

import numpy as np

from prepare import ROOT, canonical, save_json
from align_books import align
from causal_entries import write_signals
from verify import run as verify


def run(parent, folder):
    base=parent/folder;out=base/'forward';out.mkdir(exist_ok=True)
    spec_path=base/'candidate-spec.json';frozen_hash=hashlib.sha256(spec_path.read_bytes()).hexdigest()
    spec=json.loads(spec_path.read_text());start,end=spec['future_from_ms'],spec['future_to_ms']
    if (out/'exact-results.json').exists():raise ValueError('FORWARD_RESULT_ALREADY_EXISTS')
    if end-time.time()*1000>3600000:raise ValueError('FORWARD_WAIT_EXCEEDS_1H_BOUND')
    lock=out/'worker.lock'
    fd=os.open(lock,os.O_CREAT|os.O_EXCL|os.O_WRONLY);os.write(fd,str(os.getpid()).encode());os.close(fd)
    def status(phase,**extra):
        save_json(out/'status.new',{'phase':phase,'updated_ms':int(time.time()*1000),'pid':os.getpid(),
                  'from_ms':start,'to_ms':end,'orders_sent':0,'new_WS':0,'spec_sha256':frozen_hash,**extra})
        os.replace(out/'status.new',out/'status.json')
    try:
        while time.time()*1000<end+5000:
            status('WAITING_FOR_FROZEN_WINDOW_END')
            time.sleep(min(30,max(.1,(end+5000-time.time()*1000)/1000)))
        if hashlib.sha256(spec_path.read_bytes()).hexdigest()!=frozen_hash:raise ValueError('SPEC_CHANGED_AFTER_FREEZE')
        status('EXTRACTING_EXISTING_RECORDING')
        c=sqlite3.connect('file:'+str(ROOT/'data/unified/market.sqlite3')+'?mode=ro',uri=True)
        c.execute('PRAGMA query_only=ON');latest=c.execute('SELECT MAX(last_recv_ms) FROM strategy_tape_chunks').fetchone()[0];c.close()
        if latest is None or latest<end:raise ValueError('RECORDING_NOT_THROUGH_WINDOW_END')
        canonical(out,start-1800000,end,'quotes')
        z=dict(np.load(out/'quotes_BTC.npz'));z['dataset']=np.full(len(z['t']),2,dtype=np.int8)
        z,counts,manifest=align(z,int(z['t'][0])-1000,end)
        save_json(out/'aligned-book-coverage.json',counts);save_json(out/'book-source-chunks.json',manifest)
        np.savez_compressed(out/'input_BTC.npz',**z)
        assert int(z['t'][-1])==end//1000*1000,'INCOMPLETE_FUTURE_GRID'
        write_signals(out,z,spec['entry_configs'])
        a=np.column_stack([z['t']/1000,np.floor(z['hl']*10+1e-7),np.floor(z['hl_low']*10+1e-7),
                           np.ceil(z['hl_high']*10-1e-7),z['hl_age'],z['bf_age'],z['gaps'],
                           (z['t']>=start).astype(int)]).astype('<f8')
        a.tofile(out/'market_BTC.bin');os.link(out/'market_BTC.bin',out/'market-later_BTC.bin')
        for f in ('candidate-spec.json','parameters.json','cases.txt'):shutil.copy2(base/f,out/f)
        status('REPLAYING_UNCHANGED_CANDIDATE')
        subprocess.run([str(parent/'search'),str(out/'market_BTC.bin'),str(out/'signals_BTC.bin'),
                        str(out/'results.csv'),'cases',str(out/'cases.txt'),'814.598','.000476041580087574',
                        spec['risk_profile'],str(spec['margin']),str(spec['leverage'])],check=True,capture_output=True)
        verify(parent,str(Path(folder)/'forward'),('later',))
        status('COMPLETE',note='Forward SIM only. Full horizon reserved for exits. Small samples cannot establish WR80.')
    except Exception as e:
        status('BLOCKED',reason=str(e));raise
    finally:lock.unlink(missing_ok=True)


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--out',type=Path,required=True);p.add_argument('--folder',required=True)
    a=p.parse_args();run(a.out,a.folder)
