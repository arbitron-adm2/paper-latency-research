import copy
import json
import tempfile
import unittest
import zlib
from pathlib import Path
from marketdb.core import connect
from marketdb.detector import Detector
from marketdb.latency_gate import TimingGate, validate
from marketdb.research import replay
from marketdb.research_schema import init
from marketdb.timeline import available, events

BASE = json.loads((Path(__file__).resolve().parents[1] / 'configs/lead-lag.json').read_text())


def pressure(t=10000, pending=0, **extra):
    snap = {'ts': t, 'ok': True, 'stale': False, 'totalPendingGas': pending,
            'totalQueuedGas': pending, 'totalSubmittedGas': 0, 'pendingCount': int(pending > 0),
            'queueRevision': 'epoch:1', 'chain': {'blocks': [
                {'number': i + 1, 'timestampMs': t - 2000 + i * 1000,
                 'papertradeGasUsed': 100000, 'kind': 'normal'} for i in range(3)]}}
    snap.update(extra)
    return {'time_ms': t, 'recv_ms': t, 'record_id': 100, 'sub': -1, 'kind': 'pressure',
            'symbol': None, 'venue': 'paper', 'pressure': snap, 'source_ms': t}


def quote(n, t, venue, mid=100):
    return {'record_id': n, 'sub': 0, 'source_id': 1, 'time_ms': t, 'recv_ms': t,
            'ts_ms': t, 'venue': venue, 'symbol': 'BTC', 'kind': 'bbo',
            'bid': mid - .01, 'ask': mid + .01}


def config(enabled=True):
    c = copy.deepcopy(BASE)
    c.update(warmup_ms=0, cooldown_ms=1, threshold_bps=1, baseline_tau_ms=100000,
             max_quote_age_ms=1000, horizons_ms=[100, 300], entry_delay_ms=10,
             exit_delay_ms=10, hedge_delay_ms=5)
    if enabled:
        c['latency_gate'] = {'enabled': True}
    return c


def samples():
    return [quote(1, 10000, 'hl'), quote(2, 10001, 'binance_futures'),
            quote(3, 10002, 'binance_spot'), quote(4, 10100, 'binance_spot', 101),
            quote(5, 10101, 'binance_futures', 101), quote(6, 10200, 'hl', 100.5),
            quote(7, 10450, 'binance_futures', 101.1)]


class TimingGateTests(unittest.TestCase):
    def gate(self, **overrides):
        return TimingGate({'enabled': True, **overrides})

    def test_config_rejects_bool_nonfinite_negative_and_typos(self):
        for patch in ({'enabled': 1}, {'entry_budget_ms': True}, {'safety_factor': float('nan')},
                      {'entry_floor_ms': -1}, {'min_blocks': 1}, {'window_blocks': 2.5},
                      {'wrong_name': 1}, {'exit_budget_ms': float('inf')}):
            with self.subTest(patch=patch), self.assertRaises(ValueError): validate(patch)

    def test_empty_queue_uses_labelled_floors_not_zero_guaranteed_execution(self):
        g = self.gate(); g.observe(pressure())
        out = g.evaluate(10001)
        self.assertTrue(out['allow']); self.assertEqual(out['entry_ms'], 3000)
        self.assertEqual(out['exit_ms'], 10000)
        self.assertEqual(out['economics'], 'NOT_MODELLED')
        self.assertEqual(out['method'], 'configured_floors_empty_queue_v1')
        self.assertIn('NOT_QUANTILE', out['estimate_kind'])

    def test_pending_uses_paper_drain_and_distinct_exit_budget(self):
        g = self.gate(entry_budget_ms=10000, signal_ttl_ms=10000, exit_budget_ms=12000)
        g.observe(pressure(pending=300000))
        out = g.evaluate(10001)
        self.assertEqual(out['drain_gas_per_second'], 100000)
        self.assertEqual(out['entry_ms'], 7500)
        self.assertEqual(out['exit_ms'], 14500)
        self.assertEqual(out['reasons'], ['EXIT_DELAY_BUDGET'])
        self.assertFalse(out['allow'])

    def test_current_ordinary_blocks_and_string_gas_do_not_require_queue_revision(self):
        e = pressure(pending=300000)
        del e['pressure']['queueRevision']
        for block in e['pressure']['chain']['blocks']:
            block['kind'] = 'ordinary'
            block['papertradeGasUsed'] = str(block['papertradeGasUsed'])
            block['gasLimit'] = '3000000'
        g = self.gate(); g.observe(e)
        out = g.evaluate(10001)
        self.assertEqual(out['drain_gas_per_second'], 100000)
        self.assertNotIn('UNKNOWN_BLOCK_KIND', out['reasons'])
        self.assertIsNone(out['evidence']['queue_revision'])

    def test_unknown_stale_unhealthy_and_future_fail_closed(self):
        g = self.gate()
        self.assertEqual(g.evaluate(10000)['reasons'], ['PRESSURE_MISSING'])
        for snap, now, reason in [(pressure(), 14000, 'PRESSURE_STALE'),
                                  (pressure(stale=True), 10000, 'INVALID_OR_UNHEALTHY_PRESSURE'),
                                  (pressure(), 8000, 'PRESSURE_FROM_FUTURE'),
                                  (pressure(totalPendingGas=float('nan')), 10000, 'INVALID_OR_UNHEALTHY_PRESSURE')]:
            with self.subTest(reason=reason):
                other = self.gate(); other.observe(snap)
                out = other.evaluate(now)
                self.assertFalse(out['allow']); self.assertIn(reason, out['reasons'])
                self.assertIsNone(out['entry_ms'])

    def test_stalled_insufficient_and_stale_block_history(self):
        cases = []
        e = pressure(pending=1000); e['pressure']['chain']['blocks'] = []
        cases.append((e, 'INSUFFICIENT_DRAIN_HISTORY'))
        e = pressure(pending=1000)
        for b in e['pressure']['chain']['blocks']: b['papertradeGasUsed'] = 0
        cases.append((e, 'DRAIN_STALLED'))
        e = pressure(pending=1000)
        for b in e['pressure']['chain']['blocks']: b['timestampMs'] -= 6000
        cases.append((e, 'BLOCK_HISTORY_STALE'))
        for e, reason in cases:
            with self.subTest(reason=reason):
                g = self.gate(); g.observe(e)
                self.assertEqual(g.evaluate(10000)['reasons'], [reason])

    def test_source_regression_and_block_revision_conflict_are_not_refreshed_away(self):
        g = self.gate(); g.observe(pressure())
        g.observe(pressure(9999))
        self.assertIn('PRESSURE_SOURCE_REGRESSION', g.evaluate(10000)['reasons'])
        g = self.gate(); g.observe(pressure())
        e = pressure(10001); e['pressure']['chain']['blocks'][0]['number'] = 1
        g.observe(e)
        self.assertIn('BLOCK_REVISION_CONFLICT', g.evaluate(10001)['reasons'])

    def test_repeated_blocks_are_counted_once_and_checkpoint_is_deterministic(self):
        g = self.gate(entry_budget_ms=20000, signal_ttl_ms=20000)
        e = pressure(pending=1000); g.observe(e); g.observe(copy.deepcopy(e))
        out = g.evaluate(10001)
        self.assertEqual(out['sample_blocks'], 3)
        clone = TimingGate(g.config, json.loads(json.dumps(g.state)))
        self.assertEqual(out, clone.evaluate(10001))

    def test_inconsistent_gas_and_coverage_gaps_are_unknown(self):
        e = pressure(pending=1000, totalQueuedGas=9000)
        g = self.gate(); g.observe(e)
        self.assertIn('INCONSISTENT_PENDING_GAS', g.evaluate(10000)['reasons'])
        e = pressure(pending=1000); e['pressure']['chain']['blocks'][-1]['number'] = 5
        g = self.gate(); g.observe(e)
        self.assertEqual(g.evaluate(10000)['reasons'], ['BLOCK_COVERAGE_GAP'])

    def test_numeric_corruption_and_overflow_do_not_become_fast_queue(self):
        e = pressure(pending=1000, totalSubmittedGas='not-a-number')
        g = self.gate(); g.observe(e)
        self.assertIn('INVALID_GAS_OR_COUNT', g.evaluate(10000)['reasons'])
        e = pressure(pending=1000)
        for b in e['pressure']['chain']['blocks']: b['papertradeGasUsed'] = 1e308
        g = self.gate(); g.observe(e)
        self.assertEqual(g.evaluate(10000)['reasons'], ['INVALID_DRAIN_ESTIMATE'])
        self.assertEqual(g.evaluate(float('nan'))['reasons'], ['DECISION_CLOCK_INVALID'])

    def test_all_alerts_are_kept_and_future_pressure_does_not_change_prefix(self):
        c = config(); a = Detector(c); first = []
        for e in samples(): first.extend(a.on(e)[0])
        self.assertTrue(first)
        self.assertTrue(all(not s['features']['latency_gate']['allow'] for s in first))
        prefix = copy.deepcopy(first)
        a.on(pressure(11000))
        self.assertEqual(first, prefix)
        self.assertEqual(a.state['gate_counts']['suppressed'], len(first))
        self.assertTrue(a.state['pending'] or a.state['outcomes'])

    def test_same_causal_engine_with_pressure_and_restart(self):
        rows = [pressure(), *samples()]
        cfg = config(); engine = Detector(cfg); prefix = []
        for e in rows[:5]: prefix.append(engine.on(e))
        restart = Detector(cfg, json.loads(json.dumps(engine.state)))
        for e in rows[5:]: prefix.append(restart.on(e))
        other = Detector(cfg)
        self.assertEqual(prefix, [other.on(e) for e in rows])
        self.assertEqual(restart.state, other.state)

    def test_default_profile_is_unchanged_and_does_not_gain_gate_features(self):
        c = config(False); one = Detector(c); two = Detector(copy.deepcopy(c))
        self.assertEqual(one.config, c)
        a = [one.on(e) for e in samples()]; b = [two.on(e) for e in samples()]
        self.assertEqual(a, b)
        self.assertIsNone(one.gate)
        self.assertTrue(all('latency_gate' not in s['features'] for pair in a for s in pair[0]))


class TimelineGateTests(unittest.TestCase):
    def database(self, root):
        c = connect(root / 'data/unified/market.sqlite3'); init(c)
        for sid, venue in enumerate(['hl', 'paper', 'binance_futures', 'binance_spot'], 1):
            c.execute('INSERT INTO sources(id,path,format,dataset,role,venue_hint,last_recv_ms) VALUES(?,?,?,?,?,?,?)',
                      (sid, 'fixture-' + venue, 'gzip', 'observer', 'live', venue, 20000))
        return c

    def insert_quote(self, c, sid, n, t, mid=100):
        rid = (sid << 40) | n
        venue = {1: 'hl', 3: 'binance_futures', 4: 'binance_spot'}[sid]
        c.execute('INSERT INTO records VALUES(?,?,?,?,?,?,?,?,?,?,?)', (rid, sid, n, t, t, venue, 'BTC', 'bbo', None, None, zlib.compress(b'{}')))
        c.execute('INSERT INTO ticks(record_id,sub,source_id,venue,symbol,kind,ts_ms,recv_ms,bid,ask,mid) VALUES(?,?,?,?,?,?,?,?,?,?,?)',
                  (rid, 0, sid, venue, 'BTC', 'bbo', t, t, mid - .01, mid + .01, mid))
        c.execute('INSERT INTO event_clock(record_id,ingested_ms) VALUES(?,?)', (rid, t))
        return rid

    def insert_pressure(self, c, n, t, pending=0, recv=None):
        rid = (2 << 40) | n
        snap = pressure(t, pending)['pressure']
        raw = json.dumps({'recv_ms': t if recv is None else recv, 'data': {'push': {'pub': {'data': {'snapshot': snap}}}}}).encode()
        c.execute('INSERT INTO records VALUES(?,?,?,?,?,?,?,?,?,?,?)', (rid, 2, n, t, t if recv is None else recv, 'paper', None, 'pressure', None, None, zlib.compress(raw)))
        c.execute('INSERT INTO event_clock(record_id,ingested_ms) VALUES(?,?)', (rid, t))
        return rid

    def populate(self, c):
        self.insert_pressure(c, 1, 10000)
        self.insert_quote(c, 1, 1, 10000)
        self.insert_quote(c, 3, 1, 10001)
        self.insert_quote(c, 4, 1, 10002)
        self.insert_quote(c, 4, 2, 10100, 101)
        self.insert_quote(c, 3, 2, 10101, 101)
        self.insert_quote(c, 1, 2, 10200, 100.5)
        self.insert_pressure(c, 2, 10300, pending=300000)
        self.insert_quote(c, 3, 3, 10450, 101.1)
        c.commit()

    def test_tiny_batch_cursor_keeps_both_pressure_and_market_rows(self):
        with tempfile.TemporaryDirectory() as tmp:
            c = self.database(Path(tmp)); self.populate(c)
            full = list(available(c, 9000, 12000, include_pressure=True))
            after = None; seen = []
            while True:
                batch = list(available(c, 9000, 12000, after=after, limit=1, include_pressure=True))
                if not batch: break
                seen.extend(batch); e = batch[-1]; after = [e['time_ms'], e['record_id'], e['sub']]
            self.assertEqual(seen, full)
            self.assertEqual(len(full), 9)
            self.assertEqual(sum(e['kind'] == 'pressure' for e in full), 2)
            self.assertEqual(len(list(available(c, 9000, 12000))), 7)
            received = list(events(c, 9000, 12000, 'receive', include_pressure=True))
            self.assertEqual([(e['time_ms'], e['record_id'], e['sub']) for e in full],
                             [(e['time_ms'], e['record_id'], e['sub']) for e in received])
            c.close()

    def test_future_available_pressure_cannot_enter_earlier_prefix(self):
        with tempfile.TemporaryDirectory() as tmp:
            c = self.database(Path(tmp)); self.populate(c)
            prefix = list(available(c, 9000, 10200, include_pressure=True))
            self.insert_pressure(c, 3, 11000, recv=10000); c.commit()
            self.assertEqual(prefix, list(available(c, 9000, 10200, include_pressure=True)))
            c.close()

    def test_offline_replay_persists_suppressed_alerts_and_outcomes(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); c = self.database(root); self.populate(c)
            c.execute("UPDATE records SET kind='trading' WHERE venue='paper'")
            # No usable pressure: replace frames with explicit non-pressure controls.
            c.execute("UPDATE records SET raw_z=? WHERE venue='paper'", (zlib.compress(b'{"data":{}}'),)); c.commit(); c.close()
            out = replay(root, config(), 9000, 12000, run_id='offline-latency-gate')
            self.assertGreater(out['signals'], 0)
            self.assertEqual(out['latency_gate']['counts']['suppressed'], out['signals'])
            self.assertTrue(out['outcomes'])
            rows = [json.loads(x) for x in (root / 'reports/replay/offline-latency-gate/signals.jsonl').read_text().splitlines()]
            self.assertEqual(len(rows), out['signals'])
            self.assertTrue(all(json.loads(r['features_json'])['latency_gate']['reasons'] == ['PRESSURE_MISSING'] for r in rows))


if __name__ == '__main__':
    unittest.main()
