"""Sanitized evidence, with exact source units and explicit clock provenance."""
import hashlib
import json
import zlib

PRIVATE={'privatekey','private_key','mnemonic','password','passphrase','signature','signed',
         'authorization','cookie','cookies','sessionkeys','session_keys','accesstoken','refresh_token','secret'}


def clean(value):
    if isinstance(value,dict):return {k:clean(v) for k,v in value.items() if k.lower() not in PRIVATE}
    if isinstance(value,list):return [clean(x) for x in value]
    return value


def canonical(value):return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False)
def digest(value):return hashlib.sha256(canonical(value).encode()).hexdigest()


def save_event(c,obj,recv_ms,mono):
    e=obj['data'];payload=clean(e['payload']);raw=canonical(payload).encode()
    key=e.get('key') or digest([e['origin'],e['kind'],e.get('event_ms'),payload])
    c.execute('INSERT OR IGNORE INTO research_events(event_key,origin,kind,event_ms,observed_ms,recv_mono_ns,wallet,intent_id,position_id,mode,precision,raw_z) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)',
              (key,e['origin'],e['kind'],e.get('event_ms'),recv_ms,mono,e.get('wallet'),e.get('intent_id'),e.get('position_id'),e.get('mode'),e.get('precision'),zlib.compress(raw,1)))
    if e['origin']=='paper_account_history':
        eid=c.execute('SELECT id FROM research_events WHERE event_key=?',(key,)).fetchone()[0]
        p=payload;scale=float(p.get('priceScaleRaw') or 10);px=p.get('exitPriceRaw') if p.get('exitPriceRaw') is not None else p.get('entryPriceRaw')
        c.execute('INSERT OR IGNORE INTO execution_facts VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
          (eid,str(p.get('positionId')),p.get('kind'),p.get('timestampMs'),recv_ms,p.get('intentId'),p.get('symbol'),
           'long' if p.get('isLong') else 'short',float(px)/scale if px is not None else None,
           p.get('marginRaw'),p.get('adjustedPnlRaw'),p.get('userPaidFeeRaw'),p.get('paperMintedRaw'),p.get('fundingRaw'),
           p.get('txHash'),p.get('blockNumber'),'paper_account_history'))


def row_phases(row):
    """Do not label an acknowledgement as request send or absence as a close."""
    for key in ['entry_info','exit_info','close_state','close_evidence']:
        if isinstance(row.get(key),str):
            try:row[key]=json.loads(row[key])
            except (ValueError,TypeError):pass
    entry=row.get('entry_info') or {};exit=row.get('exit_info') or {};closing=row.get('close_state') or {}
    result=[]
    if entry.get('decidedAt'):result.append(('entry_decided',entry['decidedAt'],entry))
    if row.get('position_id') and row.get('status')!='duplicate':result.append(('entry_filled',row['opened_at'],row))
    decided=exit.get('decidedAt') or closing.get('decidedAt')
    if decided:result.append(('exit_decided',decided,exit or closing))
    ack=exit.get('sentAt') or closing.get('sent')
    if ack:result.append(('close_http_ack',ack,exit or closing))
    if row.get('status')=='closed' and row.get('closed_at'):
        result.append(('exit_filled',row['closed_at'],row.get('close_evidence') or row))
    return result
