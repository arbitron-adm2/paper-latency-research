"""Replay the declared sizing grid. Offline files only; no trading interfaces."""
import argparse
import csv
import gzip
import hashlib
import json
import subprocess
import time
from pathlib import Path


def qualifies(row):
    return int(row['n']) >= 30 and float(row['wr']) >= .8 and float(row['net']) > 0


def run(parent, folder='pass4'):
    out = parent / folder
    spec = json.loads((out / 'spec.json').read_text())
    source = parent / spec.get('source_folder', 'aligned')
    signals = parent / spec.get('signals', 'aligned/pass2/signals_BTC.bin')
    risk_profile = spec.get('risk_profile', 'tight')
    all_candidates = []
    counts = []
    for margin, lev in spec['sizes']:
        case_dir = out / f'm{margin}-l{lev}'
        case_dir.mkdir(exist_ok=True)
        prefix = [str(parent / 'search'), str(source / 'market-later_BTC.bin'),
                  str(signals)]
        suffix = ['814.598', '.000476041580087574', risk_profile, str(margin), str(lev)]
        train_path = case_dir / 'train.csv.gz'
        selection_path = case_dir / 'selection.json'
        if not selection_path.exists():
            selected = []
            total = 0
            digest = hashlib.sha256()
            with (case_dir / 'train.log').open('w') as log, gzip.open(train_path, 'wt') as gz:
                process = subprocess.Popen(prefix + ['/dev/stdout', 'train', '-'] + suffix,
                                           stdout=subprocess.PIPE, stderr=log, text=True)
                header = process.stdout.readline()
                gz.write(header)
                digest.update(header.encode())
                names = next(csv.reader([header]))
                for line in process.stdout:
                    gz.write(line)
                    digest.update(line.encode())
                    row = dict(zip(names, next(csv.reader([line]))))
                    total += 1
                    if qualifies(row) and int(row['unknown']) == 0:
                        selected.append(row)
                if process.wait() != 0:
                    raise RuntimeError(f'TRAIN_FAILED_{margin}')
            selection = {'selected_ms': int(time.time()*1000), 'margin': margin,
                         'leverage': lev, 'all_configs': total, 'eligible': len(selected),
                         'train_uncompressed_sha256': digest.hexdigest(), 'cases': selected}
            selection_path.write_text(json.dumps(selection, indent=2) + '\n')
            (case_dir / 'cases.txt').write_text(''.join(r['entry']+' '+r['risk']+'\n' for r in selected))
        else:
            selection = json.loads(selection_path.read_text())
        val = case_dir / 'validation.csv'
        if not val.exists():
            with (case_dir / 'validation.log').open('w') as log:
                subprocess.run(prefix + [str(val), 'cases', str(case_dir / 'cases.txt')] + suffix,
                               stderr=log, check=True)
        with val.open() as f:
            rows = list(csv.DictReader(f))
        groups = {}
        for row in rows:
            groups.setdefault((row['entry'], row['risk']), {})[row['fold']] = row
        winners = []
        for (entry, risk), folds in groups.items():
            if all(qualifies(folds[f]) for f in ('0', '1', '-1')):
                candidate = {'entry': int(entry), 'risk': int(risk), 'margin': margin,
                             'leverage': lev, 'notional': margin*lev, 'folds': folds}
                winners.append(candidate)
        (case_dir / 'qualified.json').write_text(json.dumps(winners, indent=2)+'\n')
        all_candidates.extend(winners)
        count = {'margin': margin, 'leverage': lev, 'configs': selection['all_configs'],
                 'train_eligible': selection['eligible'], 'qualified': len(winners),
                 'qualified_no_unknown': sum(all(int(c['folds'][f]['unknown']) == 0 for f in ('1','-1')) for c in winners)}
        counts.append(count)
        print(json.dumps(count), flush=True)
        (out / 'progress.json').write_text(json.dumps({'updated_ms':int(time.time()*1000), 'completed':counts},indent=2)+'\n')
    all_candidates.sort(key=lambda c:(-sum(int(c['folds'][f]['unknown']) for f in ('1','-1')),
                                      float(c['folds']['1']['without_best']),
                                      float(c['folds']['1']['net'])), reverse=True)
    (out / 'qualified.json').write_text(json.dumps(all_candidates, indent=2)+'\n')
    print('FINISHED', len(all_candidates), flush=True)
    for candidate in all_candidates[:10]:
        print(json.dumps({k:v for k,v in candidate.items() if k!='folds'}),
              {f:{k:candidate['folds'][f][k] for k in ('n','wins','wr','net','unknown','liquidations','without_best')} for f in ('0','1','-1')}, flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--out', type=Path, required=True)
    parser.add_argument('--folder', default='pass4')
    args = parser.parse_args()
    run(args.out, args.folder)
