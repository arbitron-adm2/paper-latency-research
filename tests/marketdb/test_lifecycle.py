import copy
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from marketdb.core import connect
from marketdb.evidence import save_event
from marketdb.lifecycle import project_attempts, read_window
from marketdb.research import latency
from marketdb.research_schema import init


def event(n, kind, t=1000, rid='r1', **extra):
    p = {'request_id': rid, 'attempt_id': rid, 'action': 'openPosition',
         'source_recv_mono_ns': str(int(t * 1e6)),
         'source_capture': {'boot_id': 'boot', 'connection': 'conn'}}
    p.update(extra.pop('payload', {}))
    return {'id': n, 'origin': 'client_transport', 'kind': kind, 'event_ms': t,
            'observed_ms': t + 100, 'wallet': 'wallet-a', 'mode': 'live', 'payload': p, **extra}


def project(*rows):
    return project_attempts(rows)['attempts'][0]


class LifecycleTests(unittest.TestCase):
    def test_only_original_monotonic_clock_measures_roundtrip(self):
        a = event(1, 'http_request_sent')
        b = event(2, 'http_response_headers', 1040, payload={'status': 202})
        out = project(a, b)
        self.assertEqual(out['metrics']['send_to_headers']['value_ms'], 40)
        self.assertEqual(out['metrics']['send_to_headers']['quality'], 'MEASURED')
        self.assertTrue(out['exact_send_available'])
        self.assertIsNone(out['send_to_fill_ms'])
        self.assertIsNone(out['provider_terminal'])

    def test_response_alone_does_not_invent_send(self):
        out = project(event(2, 'http_response_headers', 1040))
        self.assertFalse(out['exact_send_available'])
        self.assertIsNone(out['metrics']['send_to_headers']['value_ms'])
        self.assertEqual(out['transport_state'], 'HEADERS_OBSERVED')

    def test_replays_deduplicate_but_differing_stages_are_quarantined(self):
        a, b = event(1, 'http_request_sent'), event(2, 'http_response_headers', 1050)
        duplicate = {**b, 'id': 3, 'observed_ms': 5000}
        self.assertEqual(project(a, b, duplicate)['metrics']['send_to_headers']['value_ms'], 50)
        conflict = event(4, 'http_response_headers', 1070)
        out = project(a, b, conflict)
        self.assertIsNone(out['metrics']['send_to_headers']['value_ms'])
        self.assertIn('CONFLICTING_STAGE', out['quality'])

    def test_different_attempts_are_not_joined_by_position(self):
        rows = [event(1, 'http_request_sent', rid='one', payload={'position_ids': ['17']}),
                event(2, 'http_response_headers', 1030, rid='two', payload={'position_ids': ['17']})]
        out = project_attempts(rows)['attempts']
        self.assertEqual(len(out), 2)
        self.assertTrue(all(a['metrics']['send_to_headers']['value_ms'] is None for a in out))

    def test_source_clock_missing_mismatch_and_regression(self):
        a = event(1, 'http_request_sent')
        cases = [({'source_capture': {'boot_id': 'other', 'connection': 'conn'}}, 'CLOCK_DOMAIN_MISMATCH'),
                 ({'source_recv_mono_ns': None}, 'MISSING_SOURCE_CLOCK'),
                 ({'source_recv_mono_ns': '1'}, 'MONOTONIC_REGRESSION')]
        for patch, quality in cases:
            with self.subTest(quality=quality):
                b = event(2, 'http_response_headers', 1040, payload=patch)
                self.assertEqual(project(a, b)['metrics']['send_to_headers']['quality'], quality)
                self.assertIsNone(project(a, b)['metrics']['send_to_headers']['value_ms'])

    def test_wall_clock_step_does_not_corrupt_monotonic_duration(self):
        a = event(1, 'http_request_sent')
        b = event(2, 'http_response_headers', 1040)
        b['event_ms'] = 500
        metric = project(a, b)['metrics']['send_to_headers']
        self.assertEqual(metric['value_ms'], 40)
        self.assertEqual(metric['quality'], 'MEASURED_MONOTONIC_WALL_CLOCK_STEP')

    def test_cancel_ack_and_conflict_are_not_terminal(self):
        common = {'action': 'cancel', 'intent_id': 'i1'}
        a = event(1, 'http_request_sent', payload=common)
        b = event(2, 'http_response_headers', 1020, payload={**common, 'status': 202})
        out = project(a, b)
        self.assertEqual(out['cancel_state'], 'ACKNOWLEDGED_NOT_TERMINAL')
        self.assertIsNone(out['provider_terminal'])
        body = event(3, 'http_response_body', 1030, payload={**common, 'code': 'intent_conflict'})
        out = project(a, b, body)
        self.assertEqual(out['cancel_state'], 'CONFLICT')
        self.assertIsNone(out['cancel_to_terminal_ms'])
        absent = event(4, 'wallet_event', origin='paper_wallet', payload={'request_id': None, 'removedOperationIds': ['i1'], 'openPositions': []})
        self.assertIsNone(project(a, b, absent)['provider_terminal'])

    def test_explicit_outcome_and_exact_wallet_intent_link(self):
        a = event(1, 'http_request_sent', payload={'action': 'cancel', 'intent_id': 'i1'})
        outcome = event(2, 'wallet_event', 1100, origin='paper_wallet', payload={'request_id': None, 'intentOutcomes': [['i1', 'cancelled']]})
        out = project(a, outcome)
        self.assertEqual(out['provider_terminal'], 'cancelled')
        self.assertEqual(out['cancel_state'], 'CONFIRMED_CANCELLED')
        self.assertIsNone(project(a, {**outcome, 'wallet': 'wallet-b'})['provider_terminal'])
        self.assertIsNone(project({**a, 'mode': 'simulation'}, outcome)['provider_terminal'])
        execution = event(3, 'open', 1200, origin='paper_account_history', intent_id='i1', payload={'request_id': None, 'timestampMs': 1200})
        out = project(a, outcome, execution)
        self.assertIsNone(out['provider_terminal'])
        self.assertIn('CONFLICTING_TERMINAL_OUTCOMES', out['quality'])

    def test_execution_time_is_cross_clock_proxy_not_exact_latency(self):
        a = event(1, 'http_request_sent', payload={'intent_id': 'i1'})
        accepted = event(2, 'operation_seen', origin='paper_wallet', intent_id='i1', payload={'request_id': None, 'operation': ['i1', 'openPosition', 0, True, False, '1', '1', 1000]})
        execution = event(3, 'open', 1400, origin='paper_account_history', intent_id='i1', payload={'request_id': None, 'timestampMs': 1300})
        out = project(a, accepted, execution)
        self.assertEqual(out['proxy_accepted_to_execution_ms'], 300)
        self.assertIsNone(out['accepted_to_execution_ms'])
        self.assertEqual(out['provider_terminal'], 'succeeded')

    def test_bounded_read_window_and_legacy_negative_proxy(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            c = connect(root / 'data/unified/market.sqlite3'); init(c)
            for n, (kind, t) in enumerate([('entry_decided', 1100), ('entry_filled', 1000)]):
                save_event(c, {'data': {'origin': 'bot_db', 'kind': kind, 'event_ms': t, 'position_id': '17',
                           'mode': 'live', 'precision': 'provider_or_local', 'payload': {}}}, 2000 + n, 99)
            save_event(c, {'data': {'origin': 'client_transport', 'kind': 'http_response_headers', 'event_ms': 2000,
                       'mode': 'live', 'payload': {'request_id': 'only-ack'}}}, 2002, 123)
            c.commit()
            report, rows = read_window(c, 0, 3000, 1)
            self.assertTrue(report['truncated']); self.assertEqual(len(rows), 1)
            c.close()
            out = latency(root, 0, 3000)
            self.assertFalse(out['exact_request_send_available'])
            self.assertIsNone(out['positions'][0]['decision_to_entry_ms'])
            self.assertIsNone(out['positions'][0]['proxy_decision_to_entry_ms'])
            self.assertIn('NEGATIVE_DECISION_TO_ENTRY_MS_PROXY', out['positions'][0]['quality'])


if __name__ == '__main__':
    unittest.main()
