"""Causal, opt-in workload timing gate. This is not a profit or quantile model."""
import copy
import math
import statistics

DEFAULTS = {
    'enabled': False, 'max_pressure_age_ms': 3000, 'max_source_future_ms': 1000,
    'max_block_age_ms': 5000, 'window_blocks': 20, 'min_blocks': 3,
    'entry_floor_ms': 3000, 'exit_floor_ms': 10000, 'safety_factor': 1.5,
    'entry_budget_ms': 6000, 'exit_budget_ms': 15000, 'signal_ttl_ms': 5000,
}


def number(value):
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        return None
    try:
        out = float(value)
    except (ValueError, OverflowError):
        return None
    return out if math.isfinite(out) and out >= 0 else None


def validate(value):
    if value is None:
        return None
    if not isinstance(value, dict) or set(value) - set(DEFAULTS):
        raise ValueError('Unknown latency_gate configuration')
    c = {**DEFAULTS, **copy.deepcopy(value)}
    if not isinstance(c['enabled'], bool):
        raise ValueError('latency_gate.enabled must be boolean')
    for key in set(c) - {'enabled'}:
        val = c[key]
        if isinstance(val, bool) or not isinstance(val, (int, float)) or not math.isfinite(val) or val < 0:
            raise ValueError('Invalid latency_gate.' + key)
    for key in ('window_blocks', 'min_blocks'):
        if not isinstance(c[key], int):
            raise ValueError('Integer latency_gate.' + key + ' required')
    if not 2 <= c['min_blocks'] <= c['window_blocks'] <= 100:
        raise ValueError('latency_gate blocks require 2 <= min <= window <= 100')
    if c['safety_factor'] < 1 or any(c[k] <= 0 for k in ('max_pressure_age_ms', 'max_block_age_ms', 'entry_budget_ms', 'exit_budget_ms', 'signal_ttl_ms')):
        raise ValueError('Positive latency budgets/ages and safety factor >= 1 required')
    return c


class TimingGate:
    def __init__(self, config, state=None):
        self.config = validate(config)
        self.state = state if state is not None else {'snapshot': None, 'blocks': {}, 'last_source_ms': None}

    def observe(self, event):
        """Only feed events available on the SAME ordered timeline as quotes."""
        cfg = self.config
        snap = event.get('pressure')
        meta = {k: event.get(k) for k in ('record_id', 'sub', 'time_ms', 'recv_ms', 'source_ms')}
        bad = []
        if not isinstance(snap, dict):
            snap = {}
            bad.append('MALFORMED_PRESSURE')
        ts = number(snap.get('ts'))
        pending = number(snap.get('totalPendingGas'))
        if ts is None or pending is None or snap.get('ok') is not True or snap.get('stale') is not False:
            bad.append('INVALID_OR_UNHEALTHY_PRESSURE')
        if number(event.get('time_ms')) is None or number(event.get('recv_ms')) is None:
            bad.append('MISSING_LOCAL_PRESSURE_CLOCK')
        previous = self.state.get('last_source_ms')
        if previous is not None and ts is not None and ts < previous:
            bad.append('PRESSURE_SOURCE_REGRESSION')
        if ts is not None and previous is not None and ts == previous:
            old = self.state.get('snapshot') or {}
            if old.get('pending_gas') is not None and pending != old['pending_gas']:
                bad.append('CONFLICTING_PRESSURE_REVISION')
        if ts is not None and not bad:
            self.state['last_source_ms'] = ts
        queued, submitted = number(snap.get('totalQueuedGas')), number(snap.get('totalSubmittedGas'))
        for key in ('totalQueuedGas', 'totalSubmittedGas', 'pendingCount'):
            if key in snap and number(snap[key]) is None:bad.append('INVALID_GAS_OR_COUNT')
        if queued is not None and submitted is not None and pending is not None and abs(queued + submitted - pending) > 1:
            bad.append('INCONSISTENT_PENDING_GAS')
        count = number(snap.get('pendingCount'))
        if pending == 0 and count is not None and count > 0:
            bad.append('INCONSISTENT_PENDING_COUNT')
        blocks = (snap.get('chain') or {}).get('blocks') if isinstance(snap.get('chain'), dict) else None
        if blocks is not None and not isinstance(blocks, list):
            bad.append('MALFORMED_BLOCKS')
        normalized = {}
        for block in blocks if isinstance(blocks, list) else []:
            if not isinstance(block, dict):
                bad.append('MALFORMED_BLOCK')
                continue
            kind = block.get('kind')
            ordinary = kind in ('ordinary', 'normal', 'small')
            if not ordinary and kind not in ('big', 'large'):
                bad.append('UNKNOWN_BLOCK_KIND')
                continue
            n, at, gas = (number(block.get(k)) for k in ('number', 'timestampMs', 'papertradeGasUsed'))
            if n is None or not n.is_integer() or at is None or (ordinary and gas is None):
                bad.append('MALFORMED_BLOCK')
                continue
            if ts is not None and at > ts + cfg['max_source_future_ms']:
                bad.append('FUTURE_BLOCK')
                continue
            key = str(int(n))
            item = {'number': int(n), 'timestamp_ms': at, 'paper_gas': gas, 'ordinary': ordinary}
            old = normalized.get(key) or self.state['blocks'].get(key)
            if old is not None and (any(old[k] != item[k] for k in ('number', 'timestamp_ms', 'paper_gas')) or old.get('ordinary', True) != ordinary):
                bad.append('BLOCK_REVISION_CONFLICT')
            normalized[key] = item
        if not bad:
            self.state['blocks'].update(normalized)
            recent = sorted(self.state['blocks'].values(), key=lambda x: (x['timestamp_ms'], x['number']))[-cfg['window_blocks']:]
            self.state['blocks'] = {str(b['number']): b for b in recent}
        self.state['snapshot'] = {**meta, 'source_ms': ts, 'pending_gas': pending,
                                  'queue_revision': snap.get('queueRevision') if isinstance(snap.get('queueRevision'), str) else None,
                                  'invalid': sorted(set(bad))}

    def evaluate(self, now):
        cfg = self.config
        result = {'status': 'SUPPRESSED', 'allow': False, 'reasons': [], 'entry_ms': None, 'exit_ms': None,
                  'method': 'paper_workload_heuristic_v1', 'estimate_kind': 'HEURISTIC_NOT_QUANTILE_OR_BOUND',
                  'economics': 'NOT_MODELLED', 'mode': 'SHADOW_NO_ORDERS', 'evidence': None,
                  'drain_gas_per_second': None, 'sample_blocks': 0}
        snap = self.state.get('snapshot')
        if not snap:
            result['reasons'] = ['PRESSURE_MISSING']
            return result
        result['evidence'] = {k: snap.get(k) for k in ('record_id', 'sub', 'time_ms', 'recv_ms', 'source_ms', 'queue_revision')}
        reasons = list(snap['invalid'])
        if number(now) is None:
            result['reasons'] = ['DECISION_CLOCK_INVALID']
            return result
        for name in ('time_ms', 'recv_ms', 'source_ms'):
            at = number(snap.get(name))
            if at is None:
                reasons.append('PRESSURE_CLOCK_UNKNOWN')
            elif now - at < -cfg['max_source_future_ms']:
                reasons.append('PRESSURE_FROM_FUTURE')
            elif now - at > cfg['max_pressure_age_ms']:
                reasons.append('PRESSURE_STALE')
        if reasons:
            result['reasons'] = sorted(set(reasons))
            return result
        if snap['pending_gas'] == 0:
            workload_ms = 0
            result['method'] = 'configured_floors_empty_queue_v1'
        else:
            coverage = sorted(self.state['blocks'].values(), key=lambda x: x['number'])
            if any(b['number'] != a['number'] + 1 for a, b in zip(coverage, coverage[1:])):
                result['reasons'] = ['BLOCK_COVERAGE_GAP']
                return result
            blocks = sorted((b for b in coverage if b.get('ordinary', True)), key=lambda x: (x['timestamp_ms'], x['number']))
            result['sample_blocks'] = len(blocks)
            if len(blocks) < cfg['min_blocks']:
                result['reasons'] = ['INSUFFICIENT_DRAIN_HISTORY']
                return result
            if snap['source_ms'] - blocks[-1]['timestamp_ms'] > cfg['max_block_age_ms']:
                result['reasons'] = ['BLOCK_HISTORY_STALE']
                return result
            gaps = [b['timestamp_ms'] - a['timestamp_ms'] for a, b in zip(blocks, blocks[1:])]
            if any(x <= 0 for x in gaps) or any(b['number'] <= a['number'] for a, b in zip(blocks, blocks[1:])):
                result['reasons'] = ['INVALID_BLOCK_ORDER']
                return result
            duration_s = (blocks[-1]['timestamp_ms'] - blocks[0]['timestamp_ms'] + statistics.median(gaps)) / 1000
            if not math.isfinite(duration_s) or duration_s <= 0:
                result['reasons'] = ['INVALID_DRAIN_ESTIMATE']
                return result
            drain = sum(b['paper_gas'] for b in blocks) / duration_s
            if not math.isfinite(drain):
                result['reasons'] = ['INVALID_DRAIN_ESTIMATE']
                return result
            result['drain_gas_per_second'] = drain
            if drain <= 0:
                result['reasons'] = ['DRAIN_STALLED']
                return result
            workload_ms = cfg['safety_factor'] * snap['pending_gas'] / drain * 1000
        entry = cfg['entry_floor_ms'] + workload_ms
        exit_delay = cfg['exit_floor_ms'] + workload_ms
        if not math.isfinite(entry + exit_delay):
            result['reasons'] = ['ESTIMATE_OVERFLOW']
            return result
        result.update(entry_ms=entry, exit_ms=exit_delay)
        if entry > cfg['entry_budget_ms']: reasons.append('ENTRY_DELAY_BUDGET')
        if entry >= cfg['signal_ttl_ms']: reasons.append('EDGE_LIFETIME_BUDGET')
        if exit_delay > cfg['exit_budget_ms']: reasons.append('EXIT_DELAY_BUDGET')
        result['reasons'] = reasons
        result['allow'] = not reasons
        result['status'] = 'ALLOW_SHADOW' if not reasons else 'SUPPRESSED'
        return result
