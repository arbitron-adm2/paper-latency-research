import gzip
import json
import tempfile
import unittest
from pathlib import Path

from marketdb.core import Warehouse
from marketdb.streams import profile
from marketdb.wire import frame


class StreamingOptimizationTests(unittest.TestCase):
    def test_raw_numbers_and_receive_clock_survive_without_json_roundtrip(self):
        raw=b'{"price":81918.500000000000000001,"qty":1e-300,"msg":"ok"}'
        encoded=frame(raw=raw,recv_ms=1792493401123.456,recv_mono_ns=987654321)
        self.assertIn(raw,encoded)
        d=json.loads(encoded);self.assertEqual(d['recv_ms'],1792493401123.456)
        self.assertEqual(d['recv_mono_ns'],987654321)
        self.assertEqual(encoded.count(b'\n'),1)
        pretty=b'{\n "price": 81918.5,\n "qty": 2\n}'
        one=frame(raw=pretty)
        self.assertEqual(one.count(b'\n'),1);self.assertEqual(json.loads(one)['data'],json.loads(pretty))

    def test_profile_contains_six_quote_pairs_all_trades_and_no_depth(self):
        p=profile();text=json.dumps(p)
        self.assertFalse(p['orderbook_depth']);self.assertFalse(p['downsampling'])
        self.assertNotIn('@depth',text);self.assertNotIn('l2Book',text)
        self.assertEqual({(x['subscription']['coin'],x['subscription']['type']) for x in p['hl']['subscriptions']},
                         {(c,k) for c in ['BTC','ETH'] for k in ['bbo','trades']})
        for symbol in ['btcusdt','ethusdt']:
            self.assertIn(symbol+'@bookTicker',p['binance_futures_quotes'])
            self.assertIn(symbol+'@aggTrade',p['binance_futures_trades'])
            self.assertIn(symbol+'@bookTicker',p['binance_spot'])
            self.assertIn(symbol+'@aggTrade',p['binance_spot'])
        self.assertIn('/market/stream?',p['binance_futures_trades'])
        self.assertEqual(p['paper_channels'],['pressure:current','public:trades','trading:current'])

    def test_active_file_polling_writes_no_metadata_until_rotation(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);live=root/'data/live';live.mkdir(parents=True)
            path=live/'a.gz';f=gzip.open(path,'wb');f.write(frame(raw=b'{"stream":"btcusdt@bookTicker","data":{"s":"BTCUSDT","b":"100","a":"101"}}'));f.flush()
            state=live/'status.json'
            state.write_text(json.dumps({'active_chunks':[{'path':str(path),'source':'binance','rows':1}]}))
            w=Warehouse(root)
            try:
                a=w.live_sources();changes=w.db.total_changes
                for _ in range(20):self.assertEqual(w.live_sources(),a)
                self.assertEqual(w.db.total_changes,changes)
                self.assertEqual(w.ingest(a[0]),1)
                f.write(frame(raw=b'{"stream":"btcusdt@bookTicker","data":{"s":"BTCUSDT","b":"102","a":"103"}}'));f.flush()
                self.assertEqual(w.ingest(w.live_sources()[0]),1)
                self.assertEqual(w.db.execute('SELECT cursor FROM sources').fetchone()[0],2)
                path2=live/'b.gz'
                with gzip.open(path2,'wb') as g:g.write(frame(raw=b'{"data":{}}'))
                state.write_text(json.dumps({'active_chunks':[{'path':str(path2),'source':'binance','rows':1}]}))
                b=w.live_sources();self.assertNotEqual(a,b)
                self.assertGreater(w.db.total_changes,changes)
            finally:
                f.close()
                for rd in w.readers.values():rd.close()
                w.db.close()


if __name__=='__main__':unittest.main()
