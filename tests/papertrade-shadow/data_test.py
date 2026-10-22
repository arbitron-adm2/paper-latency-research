import importlib.util
import json
from pathlib import Path
import socket
import unittest

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('shadow_data', ROOT / 'apps/papertrade-shadow/data.py')
data = importlib.util.module_from_spec(spec)
spec.loader.exec_module(data)

class DataTests(unittest.TestCase):
    def setUp(self):
        self.original = socket.socket
        def deny(*args, **kwargs): raise AssertionError('OFFLINE_NETWORK_DENIED')
        socket.socket = deny

    def tearDown(self):
        socket.socket = self.original

    def test_mutation_and_rpc_default_deny(self):
        for path, method in [('/intents', 'POST'), ('/intents', 'GET'),
                             ('/state/trading', 'POST'), ('https://evil.example', 'GET'),
                             ('/state/user/live?wallet=secret', 'GET')]:
            with self.assertRaises(PermissionError): data.PublicReader().get(path, method)

    def test_no_private_or_production_output(self):
        with self.assertRaises(PermissionError): data.validate_output(ROOT / 'apps/vchera2/new-file')
        with self.assertRaises(PermissionError): data.validate_dataset(ROOT / 'apps/vchera2/.runtime')
        with self.assertRaises((PermissionError, FileExistsError)): data.validate_output(ROOT / 'reports')

    def test_real_shape_futures_not_spot(self):
        row = {'recv_ms': 100, 'data': {'stream': 'btcusdt@bookTicker', 'data': {
            's': 'BTCUSDT', 'ps': 'BTCUSDT', 'b': '81800', 'a': '81801', 'E': 90}}}
        self.assertIsNone(data.canonical(row, 'binance', 1))  # ps alone proves nothing
        context = {'verified': True, 'venue': 'bf', 'endpoint': 'wss://fstream.binance.com/stream',
                   'evidence': 'synthetic reviewed capture/source binding for this fixture'}
        event = data.canonical(row, 'binance', 1, context)
        self.assertEqual(event['venue'], 'bf')
        self.assertEqual(event['quality']['depth'], 'missing')
        self.assertEqual(event['event_ms'], 90)
        del row['data']['data']['ps']
        self.assertEqual(data.canonical(row, 'binance', 1, context)['venue'], 'bf')
        row['data']['data']['ps'] = 'BTCUSDT'
        spot = {**context, 'venue': 'bs', 'endpoint': 'wss://stream.binance.com/stream'}
        self.assertEqual(data.canonical(row, 'binance', 1, spot)['venue'], 'bs')
        self.assertIsNone(data.canonical(row, 'binance', 1, {**spot, 'venue': 'bf'}))
        self.assertIsNone(data.canonical(row, 'binance', 1, {**context, 'verified': False}))

    def test_missing_depth_is_not_fabricated(self):
        row = {'recv_ms': 100, 'data': {'channel': 'bbo', 'data': {'coin': 'BTC', 'time': 90,
               'bbo': [{'px': '81800', 'sz': '1'}, {'px': '81801', 'sz': '3'}]}}}
        event = data.canonical(row, 'hl', 2)
        self.assertNotIn('bids', event['payload'])
        self.assertNotIn('imb', event['payload'])
        self.assertEqual(len(event['raw_sha256']), 64)
        self.assertEqual(event, data.canonical(row, 'hl', 2))

    def test_eth_native_scale_and_channel_binding(self):
        row = {'recv_ms': 100, 'data': {'stream': 'ethusdt@bookTicker', 'data': {
            's': 'ETHUSDT', 'b': '3000.00', 'a': '3000.01', 'E': 90}}}
        context = {'verified': True, 'venue': 'bf', 'endpoint': 'wss://fstream.binance.com/stream',
                   'evidence': 'fixture source binding'}
        event = data.canonical(row, 'binance', 1, context)
        self.assertEqual(event['market'], 'ETH')
        self.assertEqual(event['payload']['priceScale'], '100')
        self.assertIn('NOT-raw-wire', event['quality']['hash_kind'])
        row['data']['stream'] = 'btcusdt@bookTicker'
        self.assertIsNone(data.canonical(row, 'binance', 1, context))

if __name__ == '__main__': unittest.main(verbosity=2)
