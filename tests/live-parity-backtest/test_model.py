import copy
import importlib.util
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('parity_model', ROOT / 'apps/live-parity-backtest/model.py')
m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m)


class CausalModelTest(unittest.TestCase):
    def test_first_attempt_cancel_and_missing_queue_are_distinct(self):
        self.assertTrue(m.successful_cancel({'status': 'cancelled', 'reason': 'сигнал пропал в очереди'}))
        self.assertTrue(m.successful_cancel({'status': 'cancelled', 'reason': 'сигнал пропал в очереди (отмена прошла с 5-й попытки)'}))
        self.assertFalse(m.successful_cancel({'status': 'cancelled', 'reason': 'заявка исчезла из очереди платформы'}))
        self.assertFalse(m.successful_cancel({'status': 'closed', 'reason': 'сигнал пропал в очереди'}))

    def test_future_quote_is_never_read(self):
        q = m.Quotes([{'recv': 1000, 'source': 950, 'bid': 100, 'ask': 101},
                      {'recv': 2000, 'source': 1950, 'bid': 200, 'ask': 201}])
        self.assertEqual(q.at(1999), 100.5)
        self.assertIsNone(q.at(999))
        self.assertIsNone(q.at(4000))

    def test_source_regression_does_not_overwrite_quote(self):
        q = m.Quotes([{'recv': 1000, 'source': 950, 'bid': 100, 'ask': 101},
                      {'recv': 1100, 'source': 900, 'bid': 200, 'ask': 201}])
        self.assertEqual(q.at(1100), 100.5)

    def test_lag_uses_only_older_quote(self):
        q = m.Quotes([{'recv': 1000, 'source': 950, 'bid': 100, 'ask': 101},
                      {'recv': 2000, 'source': 1950, 'bid': 200, 'ask': 201}])
        self.assertEqual(q.at(2100, 250), 100.5)

    def test_training_cannot_see_validation_labels(self):
        rows = []; quotes = []
        for i in range(9):
            start = 10000 + i * 20000
            r = {'id': i, 'position_id': str(i), 'status': 'closed', 'entry': 100.5, 'exit': 100.5,
                 'opened_at': start + 4000, 'closed_at': start + 14000,
                 'observed': {'entry': start + 5000, 'exit': start + 15000},
                 'entry_info': {'decidedAt': start, 'filledAt': start + 4000},
                 'exit_info': {'decidedAt': start + 6000, 'filledAt': start + 14000}}
            rows.append(r)
            for t in range(start, start + 18000, 500):
                quotes.append({'recv': t, 'source': t, 'bid': 100, 'ask': 101})
        q = m.Quotes(quotes); split = rows[8]['entry_info']['decidedAt']
        a = m.train(rows, q, split)
        changed = copy.deepcopy(rows)
        changed[8]['entry_info']['filledAt'] += 500000
        changed[8]['entry'] = 9999999
        self.assertEqual(a, m.train(changed, q, split))
        self.assertNotIn(8, a['trainIds'])

    def test_unobserved_close_cannot_enter_train(self):
        rows = [{'id': i, 'position_id': str(i), 'status': 'closed', 'closed_at': 90,
                 'observed': {'exit': 95 if i < 7 else 101}} for i in range(8)]
        # The eighth close is on-chain in train, but not known locally until after split.
        self.assertRaisesRegex(ValueError, 'INSUFFICIENT_TRAIN_LABELS', m.train, rows, m.Quotes([]), 100)

    def test_loss_and_separate_win_fee(self):
        params = {'instruments': [[0] * 19], 'winFeeRaw': str(2 * 10**16)}
        row = params['instruments'][0]
        row[12] = str(15000 * 10**18); row[13] = str(815 * 10**18)
        row[17] = str(10**17); row[18] = str(100000 * 10**18)
        loss = m.cash(20, 100, 80000, 79960, True, params)
        self.assertAlmostEqual(loss['net'], -1)
        win = m.cash(20, 100, 80000, 80040, True, params)
        self.assertGreater(win['adjusted'], 0)
        self.assertAlmostEqual(win['fee'], win['adjusted'] * .02)
        self.assertAlmostEqual(win['net'], win['adjusted'] - win['fee'])
        self.assertEqual(m.cash(20, 100, 80000, 80001, True, params)['net'], 0)
        self.assertEqual(m.cash(20, 100, 80000, 40000, True, params)['net'], -20)


if __name__ == '__main__':
    unittest.main()
