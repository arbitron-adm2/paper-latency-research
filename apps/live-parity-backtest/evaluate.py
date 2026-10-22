"""Evaluate a frozen chronological model; recorded decisions are conditioning, not predictions."""
import argparse
import hashlib
import json
import random
import sqlite3
import statistics
from pathlib import Path
from model import Quotes, cash, closed, mae, observed_net, quantile, train


def evaluate(folder, trials=1000):
    plan = json.loads((folder / 'plan.json').read_text())
    rows = json.loads((folder / 'labels.json').read_text())
    params = json.loads((folder / 'source/apps/papertrade-shadow/native-parameters.json').read_text())
    preset = json.loads((folder / 'source/apps/papertrade-shadow/queue-pools/vlad-preset.json').read_text())
    c = sqlite3.connect((folder / 'quotes.sqlite').resolve().as_uri() + '?mode=ro', uri=True)
    c.row_factory = sqlite3.Row
    quotes = Quotes([dict(x) for x in c.execute("SELECT * FROM quotes WHERE venue='hl' ORDER BY recv")]); c.close()
    model = train(rows, quotes, plan['split'])
    target = folder / 'calibration.json'
    if target.exists():
        if json.loads(target.read_text()) != model:
            raise ValueError('CALIBRATION_CHANGED')
    else:
        target.write_text(json.dumps(model, indent=2))
    # Everything above the split is excluded from fitting. Cross-boundary trades are separate.
    holdout = [r for r in closed(rows) if r['entry_info']['decidedAt'] >= plan['split']]
    crossing = [r['id'] for r in closed(rows) if r['entry_info']['decidedAt'] < plan['split'] <= r['closed_at']]
    exact = []
    for r in closed(rows):
        pred = cash(r['margin'], r['leverage'], r['entry'], r['exit'], r['side'] == 'L', params)
        exact.append({'id': r['id'], 'observedNet': observed_net(r), 'formulaNet': pred['net'],
                      'error': pred['net'] - observed_net(r)})
    oracle = []
    for r in holdout:
        for kind, at, px in [('entry', r['opened_at'], r['entry']), ('exit', r['closed_at'], r['exit'])]:
            pred = quotes.at(at, model['quoteLagMs'])
            oracle.append({'id': r['id'], 'kind': kind, 'predicted': pred, 'actual': px,
                           'error': None if pred is None else pred - px})
    comparisons = {}
    for name in ('vlad_baseline', 'train_queue_only', 'train_queue_and_observation'):
        totals = []; entries = []; exits = []; entry_times = []; exit_times = []
        valid_counts = []; clamped = 0; draw_rows = []
        for trial in range(trials):
            rng = random.Random(model['seed'] + trial)
            total = 0; valid = 0
            for r in holdout:
                ei, xi = r['entry_info'], r['exit_info']
                if name == 'vlad_baseline':
                    at = ei['decidedAt'] + rng.choice(preset['entryMs'])
                    bucket = next(b for b in preset['exitBuckets'] if b['maxHoldMs'] is None or xi['decidedAt'] - at < b['maxHoldMs'])
                    dt = rng.choice(bucket.get('values', [bucket['medianMs']]))
                    obs = 0
                else:
                    at = ei['decidedAt'] // 1000 * 1000 + rng.choice(model['entryBlocks']) * 1000
                    dt = rng.choice(model['exitBlocks']) * 1000
                    obs = rng.choice(model['entryObservationMs']) if name == 'train_queue_and_observation' else 0
                # Conditional on the recorded exit decision, but a close cannot precede own entry visibility.
                close_at = max(xi['decidedAt'], at + obs)
                clamped += close_at != xi['decidedAt']
                xt = close_at + dt if name == 'vlad_baseline' else close_at // 1000 * 1000 + dt
                ep = quotes.at(at, model['quoteLagMs'] if name != 'vlad_baseline' else 0)
                xp = quotes.at(xt, model['quoteLagMs'] if name != 'vlad_baseline' else 0)
                entry_times.append(at - r['opened_at']); exit_times.append(xt - r['closed_at'])
                if ep is None or xp is None:
                    continue
                p = cash(r['margin'], r['leverage'], ep, xp, r['side'] == 'L', params)['net']
                entries.append(ep - r['entry']); exits.append(xp - r['exit'])
                total += p; valid += 1
                if trial == 0:
                    draw_rows.append({'id': r['id'], 'entry': ep, 'exit': xp, 'net': p,
                                      'actualNet': observed_net(r), 'entryMs': at, 'exitMs': xt})
            valid_counts.append(valid)
            # Never treat a censored execution as zero PnL.
            if valid == len(holdout):
                totals.append(total)
        comparisons[name] = {
            'conditionalFilledTrades': len(holdout), 'trials': trials,
            'fullCoverageTrials': len(totals), 'meanCoveredTrades': statistics.mean(valid_counts),
            'entryPriceMAE': mae(entries), 'exitPriceMAE': mae(exits),
            'entryTimeMAE': mae(entry_times), 'exitTimeMAE': mae(exit_times),
            'exitDecisionClampedFraction': clamped / (trials * len(holdout)),
            'netMedianFullCoverageOnly': quantile(totals, .5),
            'netScenarioP10': quantile(totals, .1), 'netScenarioP90': quantile(totals, .9),
            'firstDraw': draw_rows,
            'meaning': 'Recorded entry/exit decisions and filled-trade subset are inputs; NOT a strategy profitability backtest'}
    result = {
        'mode': 'OFFLINE_EXECUTION_CALIBRATION_NO_ORDERS', 'financialMutations': 0,
        'calibrationSha256': hashlib.sha256(target.read_bytes()).hexdigest(),
        'trainingTrades': len(model['trainIds']), 'holdoutTrades': len(holdout),
        'crossBoundaryIds': crossing, 'holdoutIds': [r['id'] for r in holdout],
        'allActualNet': sum(observed_net(r) for r in closed(rows)),
        'holdoutActualNet': sum(observed_net(r) for r in holdout),
        'actualAccountingCoverage': len(exact),
        'formulaResidual': {'mae': mae([r['error'] for r in exact]),
                            'sum': sum(r['error'] for r in exact), 'rows': exact},
        'oraclePriceDiagnostic': {'knownExecutionTimeIsInput': True, 'n': len(oracle),
                                 'covered': sum(r['error'] is not None for r in oracle),
                                 'mae': mae([r['error'] for r in oracle if r['error'] is not None]), 'rows': oracle},
        'observationLag': {k: {'n': len(a), 'p50': quantile(a, .5), 'p95': quantile(a, .95)}
                           for k, a in [('entry', model['entryObservationMs']), ('exit', model['exitObservationMs'])]},
        'comparisons': comparisons,
        'limitations': ['One retrospective holdout; no genuinely new independent live session',
                        'Sparse train cancellation outcomes; stationarity is unproven',
                        'Censored quotes are UNKNOWN and excluded with explicit coverage',
                        'No sizing extrapolation; fixed 20 USDC x100 only']}
    with (folder / 'execution-evaluation.json').open('x') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    print(json.dumps({k: v for k, v in result.items() if k not in ('comparisons', 'formulaResidual', 'oraclePriceDiagnostic')}, indent=2))
    for name, m in comparisons.items():
        print(name, json.dumps({k: v for k, v in m.items() if k not in ('firstDraw', 'meaning')}))


if __name__ == '__main__':
    p = argparse.ArgumentParser(); p.add_argument('folder', type=Path); p.add_argument('--trials', type=int, default=1000)
    a = p.parse_args(); evaluate(a.folder, a.trials)
