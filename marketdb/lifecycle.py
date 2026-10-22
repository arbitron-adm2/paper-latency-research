"""Read-only request/intent projections. Missing stages never become zero latency."""
import json
import math
import zlib
from collections import defaultdict

TRANSPORT_KINDS = ('http_request_sent', 'http_response_headers', 'http_response_body', 'http_transport_unknown')
TERMINAL = {'succeeded', 'failed', 'expired', 'cancelled'}


def finite(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def payload(row):
    raw = row.get('payload')
    if isinstance(raw, dict):
        return raw
    try:
        return json.loads(zlib.decompress(row['raw_z']))
    except (KeyError, TypeError, ValueError, zlib.error):
        return {}


def _clock(p):
    cap = p.get('source_capture') or {}
    if not isinstance(cap, dict):
        return None
    boot, connection = cap.get('boot_id'), cap.get('connection')
    return (boot, connection) if isinstance(boot, str) and boot and isinstance(connection, str) and connection else None


def _mono(p):
    value = p.get('source_recv_mono_ns')
    if isinstance(value, bool):
        return None
    if isinstance(value, int) and value >= 0:
        return value
    if isinstance(value, str) and len(value) <= 30 and value.isascii() and value.isdigit():
        return int(value)
    return None


def _stage(row, p):
    return {'event_id': row.get('id'), 'event_ms': row.get('event_ms'),
            'observed_ms': row.get('observed_ms'), 'clock_id': _clock(p),
            'mono_ns': _mono(p), 'status': p.get('status'), 'code': p.get('code'),
            'intent_id': p.get('intent_id') or row.get('intent_id'),
            'accepted_at_ms': p.get('accepted_at_ms')}


def _metric(stages, end):
    start = stages.get('http_request_sent', [])
    finish = stages.get(end, [])
    result = {'value_ms': None, 'quality': 'MISSING_STAGE', 'clock_domain': 'client_monotonic', 'event_ids': []}
    if not start or not finish:
        return result
    result['event_ids'] = [x['event_id'] for x in start + finish]
    if len(start) != 1 or len(finish) != 1:
        result['quality'] = 'CONFLICTING_STAGE'
        return result
    a, b = start[0], finish[0]
    if not a['clock_id'] or not b['clock_id'] or a['mono_ns'] is None or b['mono_ns'] is None:
        result['quality'] = 'MISSING_SOURCE_CLOCK'
    elif a['clock_id'] != b['clock_id']:
        result['quality'] = 'CLOCK_DOMAIN_MISMATCH'
    elif b['mono_ns'] < a['mono_ns']:
        result['quality'] = 'MONOTONIC_REGRESSION'
    else:
        result['value_ms'] = (b['mono_ns'] - a['mono_ns']) / 1e6
        result['quality'] = 'MEASURED'
        if finite(a['event_ms']) and finite(b['event_ms']) and abs((b['event_ms'] - a['event_ms']) - result['value_ms']) > 100:
            result['quality'] = 'MEASURED_MONOTONIC_WALL_CLOCK_STEP'
    return result


def project_attempts(rows):
    """Project a bounded evidence window, joining only exact wallet+intent identities.

    Multiple differing stages for one request are quarantined, not resolved by
    latest/min/max. Collector monotonic clocks are never substituted for client
    clocks. Provider timestamps remain explicitly cross-clock proxies.
    """
    groups = defaultdict(list)
    providers = defaultdict(lambda: {'accepted': [], 'terminal': [], 'executions': []})
    malformed = 0
    for source_row in rows:
        row = dict(source_row)
        p = payload(row)
        if not isinstance(p, dict):
            malformed += 1
            continue
        wallet = row.get('wallet')
        wallet = wallet.lower() if isinstance(wallet, str) else None
        rid = p.get('request_id')
        if row.get('origin') == 'client_transport' and row.get('kind') in TRANSPORT_KINDS:
            if isinstance(rid, str) and rid:
                groups[rid].append((row, p))
            else:
                malformed += 1
        if row.get('mode') != 'live' or not wallet:
            continue
        intent = row.get('intent_id') or p.get('intentId')
        if row.get('origin') == 'paper_wallet' and row.get('kind') == 'operation_seen':
            op = p.get('operation')
            if isinstance(op, list) and len(op) > 7 and isinstance(op[0], str) and finite(op[7]):
                providers[(wallet, op[0])]['accepted'].append({'event_id': row.get('id'), 'time_ms': op[7], 'origin': 'provider_operation'})
        if row.get('origin') == 'paper_wallet' and row.get('kind') == 'wallet_event':
            outcomes = p.get('intentOutcomes', [])
            if isinstance(outcomes, list):
                for outcome in outcomes:
                    if isinstance(outcome, list) and len(outcome) == 2:
                        iid, status = outcome
                    elif isinstance(outcome, dict):
                        iid, status = outcome.get('intentId'), outcome.get('outcome')
                    else:
                        continue
                    if isinstance(iid, str) and isinstance(status, str) and status in TERMINAL:
                        providers[(wallet, iid)]['terminal'].append({'event_id': row.get('id'), 'outcome': status, 'origin': 'provider_intent_outcome'})
        if row.get('origin') == 'paper_account_history' and isinstance(intent, str) and row.get('kind') in ('open', 'close', 'liq', 'liquidation', 'Liquidated'):
            evidence = {'event_id': row.get('id'), 'outcome': 'succeeded', 'origin': 'provider_execution_history',
                        'kind': row['kind'], 'position_id': row.get('position_id') or p.get('positionId'),
                        'source_ms': p.get('timestampMs'), 'tx_hash': p.get('txHash')}
            providers[(wallet, intent)]['terminal'].append(evidence)
            providers[(wallet, intent)]['executions'].append(evidence)
    attempts = []
    for rid in sorted(groups):
        stages = defaultdict(list)
        actions, intents, wallets, attempt_ids = set(), set(), set(), set()
        modes = set()
        position_ids = set()
        quality = set()
        for row, p in groups[rid]:
            modes.add(row.get('mode'))
            action = p.get('action')
            if isinstance(action, str):
                actions.add(action)
            if row.get('wallet'):
                wallets.add(row['wallet'].lower())
            if isinstance(p.get('attempt_id'), str):
                attempt_ids.add(p['attempt_id'])
            iid = p.get('intent_id') or row.get('intent_id')
            if isinstance(iid, str) and iid:
                intents.add(iid)
            if isinstance(p.get('position_ids'), list):
                position_ids.update(str(x) for x in p['position_ids'] if isinstance(x, (str, int)) and not isinstance(x, bool))
            stage = _stage(row, p)
            # Same immutable source stage replayed by the collector is one fact.
            fingerprint = {k: v for k, v in stage.items() if k not in ('event_id', 'observed_ms')}
            if any({k: v for k, v in old.items() if k not in ('event_id', 'observed_ms')} == fingerprint for old in stages[row['kind']]):
                continue
            stages[row['kind']].append(stage)
        if len(actions) > 1: quality.add('CONFLICTING_ACTION')
        if len(wallets) > 1: quality.add('CONFLICTING_WALLET')
        if len(modes) > 1: quality.add('CONFLICTING_MODE')
        if len(intents) > 1: quality.add('CONFLICTING_INTENT')
        if len(attempt_ids) > 1 or (attempt_ids and attempt_ids != {rid}): quality.add('CONFLICTING_ATTEMPT_ID')
        if any(len(values) > 1 for values in stages.values()): quality.add('CONFLICTING_STAGE')
        action = next(iter(actions)) if len(actions) == 1 else 'unknown'
        intent = next(iter(intents)) if len(intents) == 1 else None
        wallet = next(iter(wallets)) if len(wallets) == 1 else None
        mode = next(iter(modes)) if len(modes) == 1 else None
        source = providers.get((wallet, intent), {'accepted': [], 'terminal': [], 'executions': []}) if mode == 'live' else {'accepted': [], 'terminal': [], 'executions': []}
        terminals = {x['outcome'] for x in source['terminal']}
        if len(terminals) > 1:
            quality.add('CONFLICTING_TERMINAL_OUTCOMES')
        terminal = next(iter(terminals)) if len(terminals) == 1 else None
        metrics = {name: _metric(stages, kind) for name, kind in (
            ('send_to_headers', 'http_response_headers'), ('send_to_body', 'http_response_body'),
            ('send_to_transport_unknown', 'http_transport_unknown'))}
        identity_ambiguous = bool(quality & {'CONFLICTING_ACTION', 'CONFLICTING_WALLET', 'CONFLICTING_INTENT', 'CONFLICTING_ATTEMPT_ID', 'CONFLICTING_MODE'})
        if identity_ambiguous:
            for metric in metrics.values():
                metric.update(value_ms=None, quality='AMBIGUOUS_REQUEST_IDENTITY')
            terminal = None
        for metric in metrics.values():
            if metric['quality'] not in ('MEASURED', 'MISSING_STAGE'):
                quality.add(metric['quality'])
        has_send = len(stages.get('http_request_sent', [])) == 1
        headers = stages.get('http_response_headers', [])
        bodies = stages.get('http_response_body', [])
        conflict = any(x.get('code') == 'intent_conflict' for x in bodies)
        good_ack = any(isinstance(x.get('status'), int) and 200 <= x['status'] < 300 for x in headers)
        state = 'TRANSPORT_UNKNOWN' if stages.get('http_transport_unknown') else ('BODY_OBSERVED' if bodies else 'HEADERS_OBSERVED' if headers else 'SENT' if has_send else 'MISSING_SEND')
        cancel = 'NOT_REQUESTED'
        if action == 'cancel':
            cancel = 'CONFLICT' if conflict else 'ACKNOWLEDGED_NOT_TERMINAL' if good_ack else 'REQUESTED' if has_send else 'UNKNOWN'
            if terminal == 'cancelled': cancel = 'CONFIRMED_CANCELLED'
            elif terminal == 'succeeded': cancel = 'EXECUTED_DESPITE_CANCEL'
        accepted = [x['time_ms'] for x in source['accepted']]
        accepted += [x['accepted_at_ms'] for x in bodies if finite(x['accepted_at_ms'])]
        accepted_unique = sorted(set(accepted))
        if len(accepted_unique) > 1: quality.add('CONFLICTING_ACCEPTANCE_TIME')
        times = [x['source_ms'] for x in source['executions'] if finite(x['source_ms'])]
        # No safe clock synchronisation bound exists for server vs chain clocks.
        proxy = None
        if len(accepted_unique) == 1 and len(set(times)) == 1:
            proxy = times[0] - accepted_unique[0]
            if proxy < 0:
                proxy = None
                quality.add('NEGATIVE_PROVIDER_PROXY')
        attempts.append({'request_id': rid, 'attempt_id': rid, 'wallet': wallet, 'mode': mode, 'action': action,
                         'intent_id': intent, 'position_ids': sorted(position_ids), 'stages': dict(stages),
                         'transport_state': state, 'cancel_state': cancel, 'provider_terminal': terminal,
                         'terminal_evidence': source['terminal'], 'execution_evidence': source['executions'],
                         'accepted_at_ms': accepted_unique[0] if len(accepted_unique) == 1 else None,
                         'metrics': metrics, 'send_to_fill_ms': None, 'cancel_to_terminal_ms': None,
                         'accepted_to_execution_ms': None, 'proxy_accepted_to_execution_ms': proxy,
                         'proxy_quality': 'UNVERIFIED_SERVER_VS_CHAIN_CLOCKS' if proxy is not None else 'UNAVAILABLE',
                         'exact_send_available': has_send and not identity_ambiguous and bool(stages['http_request_sent'][0]['clock_id']) and stages['http_request_sent'][0]['mono_ns'] is not None and finite(stages['http_request_sent'][0]['event_ms']),
                         'quality': sorted(quality)})
    return {'attempts': attempts, 'malformed_events': malformed,
            'exact_request_send_available': any(a['exact_send_available'] for a in attempts),
            'scope': 'Exact request IDs only; durations use original client monotonic clock. HTTP acknowledgement and disappearance are not terminal outcomes. Provider execution is not independent RPC verification.'}


def read_window(c, since_ms, until_ms, limit=20000):
    if not finite(since_ms) or not finite(until_ms) or until_ms <= since_ms:
        raise ValueError('A finite increasing lifecycle time window is required')
    if not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= 100000:
        raise ValueError('limit must be an integer in 1..100000')
    rows = c.execute('SELECT * FROM research_events WHERE observed_ms>=? AND observed_ms<=? ORDER BY observed_ms DESC,id DESC LIMIT ?',
                     (since_ms, until_ms, limit + 1)).fetchall()
    truncated = len(rows) > limit
    selected = list(reversed(rows[:limit]))
    result = project_attempts(selected)
    result.update(since_ms=since_ms, until_ms=until_ms, evidence_limit=limit, evidence_rows=len(selected),
                  truncated=truncated, window_partial=True,
                  coverage_note='Bounded observation-time window; earlier stages and later outcomes may be outside it. Missing phases remain unknown.')
    return result, selected
