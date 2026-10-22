"""Causal quote lookup, train-only execution calibration, and integer Paper accounting."""
import bisect
import json
import math
import statistics

UNIT = 10 ** 18


def obj(x):
    return json.loads(x) if isinstance(x, str) else (x or {})


def quantile(a, p):
    a = sorted(a)
    if not a:
        return None
    i = (len(a) - 1) * p
    return a[int(i)] * (1 - i % 1) + a[math.ceil(i)] * (i % 1)


def trunc_div(a, b):
    return (1 if a >= 0 else -1) * (abs(a) // b)


def cash(margin, leverage, entry, exit_, long, params):
    """Same reconstructed contract formula, including the separately paid win fee."""
    a = round(entry * 10); m = round(exit_ * 10)
    mar = round(margin * UNIT); threshold = a // 50000
    effective = (m - threshold if m > a + threshold else a if m > a else m) if long else (
        m + threshold if m + threshold < a else a if m < a else m)
    diff = effective - a if long else a - effective
    g = trunc_div(mar * leverage * diff, a)
    if g <= 0:
        adjusted = max(g, -mar)
        return {'adjusted': adjusted / UNIT, 'fee': 0.0, 'net': adjusted / UNIT}
    row = params['instruments'][0]
    v = abs(effective - a) * UNIT // a
    y = v * int(row[12]) // UNIT
    x = 1000000 * v * int(row[13]) // UNIT
    b = UNIT * UNIT // y if y else 2 ** 256 - 1
    s = int(row[18]) * UNIT // x if x else 2 ** 256 - 1
    if b > UNIT * UNIT or s > UNIT * UNIT:
        return {'adjusted': 0.0, 'fee': 0.0, 'net': 0.0}
    factor = (UNIT - int(row[17])) * UNIT // (UNIT + b + s)
    adjusted = g * factor // UNIT
    fee = adjusted * int(params['winFeeRaw']) // UNIT
    return {'adjusted': adjusted / UNIT, 'fee': fee / UNIT, 'net': (adjusted - fee) / UNIT}


class Quotes:
    def __init__(self, rows, max_age=1500):
        self.rows = []; self.times = []; last_source = -float('inf')
        for r in rows:
            # Same-channel source regression must not replace a newer BBO.
            if r['source'] is not None and r['source'] < last_source:
                continue
            last_source = r['source'] if r['source'] is not None else last_source
            self.rows.append(r); self.times.append(r['recv'])
        self.max_age = max_age

    def at(self, t, lag=0):
        i = bisect.bisect_right(self.times, t - lag) - 1
        if i < 0:
            return None
        q = self.rows[i]
        if t - lag - q['recv'] > self.max_age or q['ask'] - q['bid'] > 6:
            return None
        # A stored event has to be available at its replay receive time.
        return math.floor((q['bid'] + q['ask']) * 5 + 1e-7) / 10


def closed(rows):
    return [r for r in rows if r['status'] == 'closed' and r.get('position_id')]


def successful_cancel(r):
    # liveTick stores just "сигнал пропал в очереди" when cancel succeeds on attempt one.
    # The "отмена прошла" suffix exists only after retries. Queue disappearance is not a cancel receipt.
    reason = r.get('reason') or ''
    return r['status'] == 'cancelled' and ('отмена прошла' in reason or reason == 'сигнал пропал в очереди')


def train(rows, quotes, split):
    # Both exchange close and local observation must be strictly before the split.
    history = [r for r in closed(rows) if max(r['closed_at'], r['observed'].get('exit', r['closed_at'])) < split]
    if len(history) < 8:
        raise ValueError('INSUFFICIENT_TRAIN_LABELS')
    entry_blocks = []; exit_blocks = []; entry_obs = []; exit_obs = []
    for r in history:
        ei = r['entry_info']; xi = r['exit_info']
        entry_blocks.append(round((ei['filledAt'] - (ei['decidedAt'] // 1000) * 1000) / 1000))
        exit_blocks.append(round((xi['filledAt'] - (xi['decidedAt'] // 1000) * 1000) / 1000))
        for kind, target, filled in [('entry', entry_obs, r['opened_at']), ('exit', exit_obs, r['closed_at'])]:
            if kind in r['observed']:
                lag = r['observed'][kind] - filled
                if not 0 <= lag <= 30000:
                    raise ValueError('INVALID_OBSERVATION_LAG')
                target.append(round(lag))
    success = sum(successful_cancel(r) and r['closed_at'] < split for r in rows)
    failed = sum(bool(r['entry_info'].get('cancelRequestedAt')) for r in history)
    candidates = []
    for lag in (0, 250, 500, 1000, 1500, 2000):
        errors = []
        for r in history:
            for t, px in [(r['opened_at'], r['entry']), (r['closed_at'], r['exit'])]:
                q = quotes.at(t, lag)
                if q is not None:
                    errors.append(abs(q - px))
        candidates.append({'lag': lag, 'n': len(errors), 'mae': statistics.mean(errors) if errors else None})
    eligible = [x for x in candidates if x['n'] >= .9 * len(history) * 2]
    if not eligible:
        raise ValueError('INSUFFICIENT_TRAIN_QUOTES')
    best = min(eligible, key=lambda x: (x['mae'], x['lag']))
    return {'trainIds': [r['id'] for r in history], 'maxLabelTime': max(r['closed_at'] for r in history),
            'entryBlocks': entry_blocks, 'exitBlocks': exit_blocks,
            'entryObservationMs': entry_obs, 'exitObservationMs': exit_obs,
            'cancelSuccesses': success, 'cancelFailures': failed,
            'cancelSuccessProbability': (success + 1) / (success + failed + 2),
            'cancelEstimator': 'Beta(1,1) posterior mean; sparse outcomes, not an identified server law',
            'quoteLagMs': best['lag'], 'quoteLagCandidates': candidates,
            'seed': 20261021, 'scope': 'BTC, margin20, leverage100, one chronological training window'}


def observed_net(r):
    evidence = r['close_evidence']
    if 'realPnl' not in evidence or 'fee' not in evidence:
        raise ValueError('MISSING_ACCOUNTING_EVIDENCE')
    return evidence['realPnl'] - evidence['fee']


def mae(values):
    return statistics.mean(abs(v) for v in values) if values else None
