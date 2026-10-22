"""Bounded extra frontend reads; record evidence without executing downloaded JavaScript."""
import datetime as dt
import json
from pathlib import Path
import re
from data import PublicReader, PUBLIC, save, digest, ROOT

out = ROOT / 'reports/shadow-20261020-01/rank-public'
out.mkdir(exist_ok=False)
reader = PublicReader()
records = []
for name in ['useTradingNetwork-D_mAUuhy.js', 'hooks-BlPPrJdK.js']:
    path = '/assets/' + name
    PUBLIC.add(path)
    at = dt.datetime.now(dt.timezone.utc).isoformat()
    try:
        b = reader.get(path)
        (out / name).write_bytes(b)
        records.append({'url': 'https://exchange.papertrade.xyz' + path, 'at_utc': at,
                        'file': name, 'sha256': digest(b), 'bytes': len(b)})
    except Exception as e:
        records.append({'url': path, 'at_utc': at, 'error': str(e)})
save(out / 'manifest.json', records)
snippets = []
for folder in [out, out.parent / 'public']:
    for file in folder.glob('*.js'):
        text = file.read_text()
        for token in ['volume', 'priority', 'queue', 'bustBuffer', 'minimumOpenNotional',
                      'maximumLeverage', 'adjustedPnl', 'winFeeRate', 'queuePosition', 'positions', 'chainId', '6cd566']:
            matches = list(re.finditer(re.escape(token), text, re.I))
            for m in matches[:8]:
                snippets.append({'file': str(file.relative_to(ROOT)), 'sha256': digest(file.read_bytes()),
                                 'token': token, 'offset': m.start(),
                                 'snippet': text[max(0, m.start()-120):m.end()+240]})
save(out / 'snippets.json', snippets)
print(json.dumps(snippets, ensure_ascii=False, indent=2))
