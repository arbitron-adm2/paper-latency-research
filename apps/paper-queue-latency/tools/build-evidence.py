#!/usr/bin/env python3
"""Static factual evidence compiler. Reads named artifacts, never executes source code."""
import argparse
import csv
import hashlib
import json
import math
import os
import statistics
import tempfile
from collections import Counter, defaultdict
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path


def canonical(v):
    return json.dumps(v, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)


def digest(v):
    return hashlib.sha256(canonical(v).encode()).hexdigest()


def finite(v):
    return isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(v)


def num(v):
    if isinstance(v, bool) or v is None or v == '': return None
    try: n = float(v)
    except (TypeError, ValueError, OverflowError): return None
    return n if math.isfinite(n) else None


def atomic(file, data):
    file.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=file.name + '.', suffix='.tmp', dir=file.parent)
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as f:
            f.write(data); f.flush(); os.fsync(f.fileno())
        os.replace(tmp, file)
    finally:
        if os.path.exists(tmp): os.unlink(tmp)


def metric(start, end, meaning, ambiguous=False):
    if not finite(start) and not finite(end): status = 'MISSING_BOTH_PHASES'
    elif not finite(start): status = 'LEFT_CENSORED_MISSING_START'
    elif not finite(end): status = 'RIGHT_CENSORED_MISSING_TERMINAL'
    elif end < start or ambiguous: status = 'AMBIGUOUS_ATTRIBUTION_OR_CLOCK'
    else: status = 'OBSERVED_CROSS_CLOCK_INTERVAL'
    return {'start_ms': start if finite(start) else None, 'end_ms': end if finite(end) else None,
            'value_ms': end - start if status == 'OBSERVED_CROSS_CLOCK_INTERVAL' else None,
            'raw_difference_ms': end - start if finite(start) and finite(end) else None,
            'status': status, 'meaning': meaning, 'terminal_resolution_ms': 1000,
            'clock_sync_verified': False, 'queue_wait_proven': False, 'calibration_eligible': False}


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--project-root', type=Path, required=True)
    p.add_argument('--empirical-root', type=Path, required=True)
    p.add_argument('--audit-root', type=Path, required=True)
    p.add_argument('--cli-review', type=Path, required=True)
    p.add_argument('--output', type=Path, required=True)
    a = p.parse_args(); out = a.output.resolve(); out.mkdir(parents=True, exist_ok=True)
    manifest = []
    def read(file):
        b = file.read_bytes(); manifest.append({'path': str(file), 'sha256': hashlib.sha256(b).hexdigest(), 'bytes': len(b)})
        return b.decode('utf-8-sig')
    reports = a.project_root / 'reports/vchera2-analysis-20261020'
    risk = json.loads(read(reports / 'risk-event-1659048.json'))
    wins = json.loads(read(reports / 'three_wins.json'))
    inherited = [json.loads(line) for line in read(a.empirical_root / 'lifecycle_casebook.jsonl').splitlines() if line]
    actual = json.loads(read(a.empirical_root / 'our_actual_positions.json'))
    tails = json.loads(read(a.audit_root / 'historical_tails.json'))
    deadline = json.loads(read(a.audit_root / 'deadline_rpc.json'))
    review = json.loads(read(a.cli_review))
    snap = json.loads(read(a.audit_root / 'pressure_snapshot.json'))
    rows = list(csv.DictReader(read(a.empirical_root / 'latency_samples.csv').splitlines()))
    fields = list(rows[0]) if rows else []
    field_counts = {k: dict(Counter(r.get(k, '') for r in rows)) for k in ('source_group', 'instrument', 'action', 'clock_pair', 'status', 'evidence_level', 'request_intent_semantics', 'source_mode')}
    dedup = {}; aliases = 0
    for row in rows:
        ids = json.loads(row['position_ids']) if row.get('position_ids') else []
        if row.get('request_intent_semantics') == 'ACTUAL_REQUEST_INTENT' and row.get('intent_id'):
            identity = [row[k] for k in ('source_group', 'series_epoch', 'execution_method', 'action', 'clock_pair', 'intent_id')] + [sorted(ids)]
        else:
            provenance = json.loads(row.get('provenance') or '[]')
            strong = sorted((x.get('sha256'), str(x.get('row'))) for x in provenance if x.get('sha256') and x.get('row') is not None)
            identity = [row['source_group'], row['execution_method'], row['clock_pair'], strong or ['UNLINKED_SAMPLE', row['sample_id']]]
        key = digest(identity)
        version = tuple(row.get(k) for k in ('start_ms', 'end_ms', 'raw_duration_ms', 'notional_usd', 'instrument', 'status'))
        bucket = dedup.setdefault(key, {})
        if version in bucket: aliases += 1
        bucket[version] = row
    qualified = []; excluded = Counter(); exclusion_reasons = Counter(); conflicts = 0
    for versions in dedup.values():
        if len(versions) != 1:
            conflicts += 1; excluded['CONFLICTING_STRONG_IDENTITY'] += len(versions); continue
        row = next(iter(versions.values()))
        value = num(row.get('raw_duration_ms')); size = num(row.get('notional_usd'))
        reasons = []
        if row.get('status') != 'MEASURED': reasons.append('INTERVAL_NOT_MEASURED_STATUS')
        if row.get('measured_qualified_any_size') != 'True': reasons.append('SOURCE_CLOCK_OR_LINK_NOT_QUALIFIED')
        if value is None or value < 0: reasons.append('DURATION_MISSING_OR_CONFLICTING')
        if size is None or size <= 0: reasons.append('NOTIONAL_UNKNOWN_OR_INVALID')
        if row.get('instrument') not in ('BTC', 'ETH'): reasons.append('ASSET_UNKNOWN')
        if row.get('source_mode') == 'simulation': reasons.append('SIMULATION_NOT_EMPIRICAL_EXECUTION')
        if reasons:
            excluded[row.get('status') or 'UNKNOWN_STATUS'] += 1; exclusion_reasons.update(reasons); continue
        # A true monotonic HTTP duration may lack wall-clock endpoints. Keep its
        # transport scope; never substitute it for executor wait.
        qualified.append(row)
    def interval_class(clock):
        if clock in ('http_monotonic_duration', 'request_start_to_response', 'request_start_to_accepted', 'cancel_request_start_to_response'): return 'TRANSPORT_NOT_EXECUTOR_WAIT'
        if 'observation' in clock or clock in ('response_to_platform_fill', 'response_to_platform_terminal', 'response_to_source_recorded_terminal'): return 'READBACK_OR_ACK_INTERVAL_NOT_SEND_TO_FILL'
        if clock.startswith('process_start'): return 'PROCESS_LIFETIME_NOT_EXECUTOR_WAIT'
        if 'source_recorded' in clock: return 'SOURCE_REPORTED_CROSS_CLOCK_NOT_CHAIN_VERIFIED'
        return 'OBSERVED_ENDPOINT_INTERVAL_CLOCK_SYNC_UNVERIFIED'
    groups = defaultdict(list)
    for row in qualified:
        key = tuple(row[k] for k in ('source_group', 'series_epoch', 'execution_method', 'instrument', 'action', 'clock_pair', 'source_mode', 'evidence_level', 'notional_usd'))
        groups[key].append(row)
    cohorts = []
    for key, samples in sorted(groups.items()):
        values = [num(r['raw_duration_ms']) for r in samples]
        chronological = sorted((num(r.get('start_ms')), r['sample_id']) for r in samples if num(r.get('start_ms')) is not None)
        cut = int(len(chronological) * .7) if len(chronological) >= 4 else 0
        while 0 < cut < len(chronological) and chronological[cut - 1][0] == chronological[cut][0]: cut -= 1
        lower = [num(r.get('duration_lower_ms')) for r in samples]; upper = [num(r.get('duration_upper_ms')) for r in samples]
        source, epoch, method, asset, action, clock, mode, evidence, size = key
        cohorts.append({'cohort_id': digest(key)[:24], 'source_group': source, 'series_epoch': epoch,
            'execution_method': method, 'asset': asset, 'action': action, 'clock_pair': clock, 'source_mode': mode,
            'evidence_level': evidence, 'notional_usd': num(size), 'measurement_class': interval_class(clock),
            'n': len(samples), 'median_ms': statistics.median(values) if len(values) >= 3 else None,
            'range_ms': [min(values), max(values)], 'terminal_precision_ms': sorted(set(r.get('terminal_precision_ms') or 'UNKNOWN' for r in samples)),
            'interval_lower_ms': min((v for v in lower if v is not None), default=None),
            'interval_upper_ms': max((v for v in upper if v is not None), default=None),
            'p99_ms': None, 'predictive_confidence': None, 'predictive_support': 'UNVALIDATED',
            'chronological_partition': {'kind': 'DESCRIPTIVE_NOT_UNTOUCHED_OOS', 'timestamped_n': len(chronological),
                'calibration_n': cut, 'holdout_n': len(chronological) - cut if cut else 0,
                'calibration_end_ms': chronological[cut - 1][0] if cut else None,
                'holdout_begin_ms': chronological[cut][0] if cut else None},
            'sample_ids_sha256': digest(sorted(r['sample_id'] for r in samples)), 'transfer_to_other_asset_size_or_method': False})
    cases = {}; duplicate_cases = 0
    for row in inherited:
        source = row.get('source_group', 'UNKNOWN')
        strong = row.get('evidence_level') == 'ACTUAL_OBSERVED' and row.get('position_id')
        identity = [source, row['position_id']] if strong else [source, row.get('family'), row.get('source_local_id'), row.get('intent_id'), row.get('case_id')]
        key = digest(identity)
        if key in cases: duplicate_cases += 1; continue
        start = row.get('decision_ms'); opened = row.get('open_ms', row.get('recorded_open_ms'))
        close_start = row.get('close_decision_ms'); terminal = row.get('terminal_ms', row.get('recorded_terminal_ms'))
        cases[key] = {'case_id': key[:24], 'source_case_id': row.get('case_id'), 'source_group': source,
            'position_id': row.get('position_id'), 'entry_intent_id': row.get('intent_id'), 'close_intent_id': None,
            'identity_strength': 'ACTUAL_POSITION_NAMESPACE' if strong else 'SOURCE_PROVENANCE_ONLY_NOT_WALLET_ATTRIBUTION',
            'wallet_ownership': 'UNKNOWN' if source == 'VLAD_TESTS' else 'SOURCE_CONTROLLED_NOT_REATTRIBUTED',
            'evidence_level': row.get('evidence_level'), 'notional': row.get('notional'),
            'close_cause': row.get('canonical_close_cause', 'UNKNOWN'), 'source_reason': row.get('source_reason'),
            'intervals': {'decision_to_fill': metric(start, opened, 'Decision, not proven actual send'),
                          'close_decision_to_terminal': metric(close_start, terminal, 'Recorded decision to terminal, cross-clock'),
                          'ack_to_terminal': metric(row.get('response_ms'), terminal, 'HTTP response is acknowledgement, not request send'),
                          'holding_period': metric(opened, terminal, 'Holding period, never executor wait')},
            'terminal_kind': 'UNKNOWN_UNLESS_PROVIDER_RECORD', 'provenance': row.get('provenance', []),
            'predecision_pressure': None, 'prediction_comparison': 'UNMATCHED_NO_CAUSAL_PRESSURE_RECORD',
            'limitations': row.get('limitations', row.get('limits', []))}
    for row in actual:
        key = digest(['OUR_TESTS', row['position_id']])
        if key in cases:
            cases[key]['close_cause'] = row.get('canonical_close_cause', 'UNKNOWN')
            cases[key]['kill_initiator'] = row.get('kill_initiator', 'UNKNOWN')
    late = list(wins['selected'])
    risk_row = risk['risk_event']
    risk_provider = next(x for x in risk['provider_confirmation'] if str(x['positionId']) == risk_row['position_id'])
    late.append({**risk_row, 'provider_record': risk_provider})
    for row in late:
        pid = row['position_id']; entry = row['entry_info']; close = row['exit_info']; provider = row['provider_record']
        key = digest(['OUR_TESTS', pid]); terminal = provider['timestampMs']
        ambiguous = pid == '1658910'
        cases[key] = {'case_id': key[:24], 'source_group': 'OUR_TESTS', 'position_id': pid,
            'entry_intent_id': None, 'close_intent_id': close.get('intentId'), 'asset': provider['symbol'],
            'identity_strength': 'POSITION_PLUS_CLOSE_INTENT_PLUS_PROVIDER_TX', 'evidence_level': 'PROVIDER_AND_LOCAL_REPORT',
            'notional_usd': float(Decimal(provider['marginRaw']) / Decimal(10**18) * Decimal(str(provider['leverage']))),
            'close_cause': 'PROTECTIVE_LATE_FILL', 'terminal_kind': provider['kind'], 'liquidation': False,
            'reported_adjusted_pnl_raw': provider['adjustedPnlRaw'], 'net_pnl_claim': None,
            'intervals': {'decision_to_fill': metric(entry.get('decidedAt'), provider.get('openedAtMs'), 'Decision attribution ambiguous without entry intent' if ambiguous else 'Decision to platform fill, not submit-to-fill', ambiguous),
                          'close_decision_to_terminal': metric(close.get('decidedAt'), terminal, 'Protective close decision to platform close'),
                          'ack_to_terminal': metric(close.get('sentAt'), terminal, 'Field sentAt is HTTP acknowledgement, NOT actual send'),
                          'holding_period': metric(provider.get('openedAtMs'), terminal, 'Holding period, never executor wait')},
            'cancel': {'requested_at_ms': entry.get('cancelRequestedAt'), 'response': entry.get('cancelFailed'), 'outcome': 'FILL_OBSERVED_NOT_CANCELLED', 'retries_reported': entry.get('cancelTries')},
            'request_rank': {'initial_reported_after_submit': entry.get('rank0'), 'last_reported_after_submit': entry.get('rank'), 'predecision_rank': None},
            'provenance': [{'file': str(reports / ('risk-event-1659048.json' if pid == '1659048' else 'three_wins.json')), 'position_id': pid, 'provider_tx_hash': provider['txHash']}],
            'predecision_pressure': None, 'prediction_comparison': 'UNMATCHED_NO_CAUSAL_PRESSURE_RECORD',
            'calibration_eligible': False, 'limitations': ['No matched entry intent and actual send clock', 'Terminal block timestamp has second precision', 'No timestamped pressure/prediction before this decision', 'Positive outcomes are protective late fills, not normal strategy exits']}
    raw_path = Path(tails['source']).resolve()
    allowed_raw = (a.project_root / 'incoming/vlad/extracted').resolve()
    if not raw_path.is_relative_to(allowed_raw) or raw_path.name != 'zamery-ocheredi.csv' or raw_path.parent.name != '08-zamery-ocheredi':
        raise ValueError('Raw queue source outside explicit artifact allowlist')
    raw_text = read(raw_path); queue_rows = list(csv.DictReader(raw_text.splitlines(), delimiter=';'))
    queue_fields = list(queue_rows[0]); qgroup = defaultdict(list)
    for index, row in enumerate(queue_rows, 2): qgroup[(row[queue_fields[2]], row[queue_fields[4]], row[queue_fields[5]])].append((index, row))
    for (kind, iid, pid), versions in qgroup.items():
        values = sorted(set(num(row[queue_fields[3]]) for _, row in versions if num(row[queue_fields[3]]) is not None))
        key = digest(['VLAD_QUEUE_OBSERVATION', kind, iid, pid])
        cases[key] = {'case_id': key[:24], 'source_group': 'VLAD_QUEUE_OBSERVATION', 'position_id': pid or None, 'source_intent_id': iid or None,
            'action': kind, 'wallet_ownership': 'UNKNOWN_SOURCE_ALIAS_NOT_ATTRIBUTION', 'evidence_level': 'SOURCE_CLIENT_OBSERVATION',
            'identity_strength': 'SOURCE_ACTION_INTENT_POSITION_ONLY', 'source_rows': [i for i, _ in versions],
            'duration_versions_ms': values, 'status': 'AMBIGUOUS_MULTIPLE_OBSERVATIONS' if len(values) > 1 else 'OBSERVED_DURATION_WITH_UNKNOWN_START_CLOCK',
            'meaning': 'cancel is accepted-to-removal lifetime, NOT cancel RTT; entry/exit recorder endpoints differ',
            'actual_submit_ms': None, 'accepted_ms': None, 'terminal_execution_ms': None, 'calibration_eligible': False,
            'predecision_pressure': None, 'prediction_comparison': 'UNMATCHED_NO_CAUSAL_PRESSURE_RECORD',
            'provenance': [{'file': str(raw_path), 'sha256': manifest[-1]['sha256']}],
            'limitations': ['Source recorder caps some samples near 120s', 'Source label LIVE is not independent onchain proof', 'Strong source IDs dedup repetitions, no arbitrary earliest/min duration selection']}
    calibration = {'status': 'INCONCLUSIVE', 'matched_prediction_terminal_n': 0, 'matched_predecision_pressure_n': 0,
        'calibration_pairs_n': 0, 'holdout_pairs_n': 0, 'coverage': {'matched_cases': 0, 'casebook_cases': len(cases)},
        'prediction_accuracy': None, 'p99_error_ms': None, 'confidence': None,
        'missing_evidence': ['Timestamped causal pressure/features recorded before each actual decision', 'Prediction emitted before that decision with model/input hashes', 'Exact request or intent-level start tied to the same terminal outcome', 'Comparable source/local/chain clock bounds', 'Independent chronological matched holdout within asset/size/method'],
        'future_leakage_checks': {'audit_pressure_recv_ms': snap['receivedMs'], 'late_case_latest_close_ms': max(x['provider_record']['timestampMs'] for x in late),
            'audit_pressure_is_future_to_late_cases': snap['receivedMs'] > max(x['provider_record']['timestampMs'] for x in late),
            'historical_future_pressure_used_as_prediction': False},
        'chronological_partitions_are_descriptive_only': True}
    offsets = {str(x.get('offset')): x['result'].get('error') is None for x in deadline['results'] if x.get('test') == 'deadline_offset'}
    deadline_summary = {'status': 'HISTORICAL_CONTRACT_SIMULATION_ONLY_NOT_THIS_GUARD', 'equality_allowed': offsets.get('0'),
        'reject_if': 'block.timestamp > signed_deadline_seconds', 'seconds_resolution': True,
        'seconds_reserve': 'Client must reserve seconds before original valid-until because equality is allowed; no deadline is constructed or extended by this headless guard.',
        'short_ttl_intake_money_test': 'NOT_PROVEN', 'live_pending_cancel_money_test': 'NOT_PROVEN', 'quick_close_guarantee': False,
        'open_cli_review_findings': [{'id': x['id'], 'priority': x['priority'], 'title': x['title'], 'status': 'OPEN_UNVERIFIED_BY_THIS_LEAF'} for x in review['findings']],
        'economic_safety_or_live_permission': False}
    case_list = sorted(cases.values(), key=lambda x: (x['source_group'], str(x.get('position_id')), x['case_id']))
    interval_status = Counter(m['status'] for c in case_list for m in c.get('intervals', {}).values())
    catalog = {'schemaVersion': 1, 'latency_csv_fields': fields, 'latency_csv_rows': len(rows), 'field_counts': field_counts,
        'qualified_unique_intervals': len(qualified), 'strong_identity_groups': len(dedup), 'exact_alias_rows_deduplicated': aliases,
        'conflicting_identity_groups_quarantined': conflicts, 'excluded_or_missing_status_counts': dict(excluded),
        'exclusion_reason_counts_nonexclusive': dict(exclusion_reasons),
        'our_coverage': {'reported_closed_rows_in_late_report': wins['all_closed_stats']['closed'], 'late_cases_reconstructed': 4, 'earlier_positions_reconstructed': len(actual), 'claim_all_closed_rows_fully_reconstructed': False},
        'casebook_cases': len(cases), 'case_source_groups': dict(Counter(x['source_group'] for x in case_list)),
        'case_interval_statuses': dict(interval_status), 'inherited_case_aliases': duplicate_cases,
        'raw_queue_rows': len(queue_rows), 'raw_queue_source_identity_groups': len(qgroup),
        'raw_queue_multiple_duration_groups': sum(len(c.get('duration_versions_ms', [])) > 1 for c in case_list if c['source_group'] == 'VLAD_QUEUE_OBSERVATION'),
        'field_semantics': {'position_id': 'Source platform identifier, not local bot row ID; source-reported Vlad records are not independently attributed',
            'close.sentAt': 'HTTP acknowledgement in late-case reports, not send start', 'opened-to-terminal': 'holding period, not executor wait',
            'source LIVE': 'source mode label, not independent receipt evidence', 'queue cancel duration': 'accepted-to-removal observed lifetime, not cancel request RTT'}}
    summary = {'schemaVersion': 1, 'kind': 'guard_empirical_summary', 'sample_rows': len(rows), 'unique_qualified_intervals': len(qualified),
        'cohorts': cohorts, 'calibration': calibration, 'deadline_protection': deadline_summary,
        'input_manifest_sha256': digest(manifest), 'scope': 'Descriptive cohorts partitioned by exact source/asset/size/clock/method/epoch, not a calibrated personal queue model',
        'historical_tail_reference': {'source': tails['source'], 'close_observation_n': 500, 'median_ms': 9781.5, 'p99_ms': 26752.3, 'max_observed_ms': 100439,
            'interval': 'Accepted-to-local-close-observation with recorder censoring; historical earliest-row policy, NOT guaranteed executor wait or limit'},
        'financial_mutations': 0}
    atomic(out / 'lifecycle_casebook.jsonl', ''.join(canonical(c) + '\n' for c in case_list))
    for name, value in [('field_catalog.json', catalog), ('empirical_summary.json', summary), ('calibration.json', calibration), ('source_manifest.json', {'files': manifest, 'sha256': digest(manifest)})]:
        atomic(out / name, json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False) + '\n')
    print(json.dumps({'casebook_cases': len(cases), 'qualified_intervals': len(qualified), 'cohorts': len(cohorts), 'calibration': calibration['status'], 'financial_mutations': 0}))


if __name__ == '__main__': main()
