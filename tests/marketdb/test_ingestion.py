import gzip
import json
import tempfile
import unittest
import zlib
from pathlib import Path
from marketdb.core import Warehouse, decode, timestamp


def frame(n):
    return json.dumps({'recv_ms': 1792480000000+n, 'data': {'stream': 'btcusdt@bookTicker',
        'data': {'e': 'bookTicker', 's': 'BTCUSDT', 'T': 1792480000000+n, 'b': '100', 'a': '102', 'B': '3', 'A': '4'}}}).encode()


class IngestionTests(unittest.TestCase):
    def setUp(self):
        # Standard-library temporary files are cleaned by the OS helper.
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.w = Warehouse(self.root)

    def tearDown(self):
        for r in self.w.readers.values(): r.close()
        self.w.db.close()
        self.tmp.cleanup()

    def source(self, path, **extra):
        return self.w.register({'path': str(path), 'format': 'gzip', 'dataset': 'observer', 'role': 'live',
                                'venue_hint': 'binance_futures', 'timezone_rule': 'epoch', **extra})

    def test_growing_gzip_partial_line_rotation_and_idempotence(self):
        path = self.root/'active.gz'
        f = gzip.open(path, 'wb'); f.write(frame(1)+b'\n'); f.flush()
        s = self.source(path)
        self.assertEqual(self.w.ingest(s['id']), 1)
        f.write(frame(2)[:40]); f.flush()
        self.assertEqual(self.w.ingest(s['id']), 0)
        f.write(frame(2)[40:]+b'\n'); f.flush()
        self.assertEqual(self.w.ingest(s['id']), 1)
        f.close(); self.w.ingest(s['id'])
        self.assertEqual(self.w.ingest(s['id']), 0)
        self.assertEqual(self.w.db.execute('SELECT COUNT(*) FROM records').fetchone()[0], 2)
        t = self.w.db.execute('SELECT bid,ask,mid,bid_size,ask_size FROM ticks LIMIT 1').fetchone()
        self.assertEqual(tuple(t), (100,102,101,3,4))
        raw = self.w.db.execute('SELECT raw_z FROM records ORDER BY id LIMIT 1').fetchone()[0]
        self.assertEqual(zlib.decompress(raw), frame(1))

    def test_resume_uses_committed_row_not_compressed_file_end(self):
        path = self.root/'resume.gz'; f = gzip.open(path, 'wb')
        f.write(frame(1)+b'\n'+frame(2)+b'\n'); f.flush()
        s = self.source(path); self.w.ingest(s['id'], 1)
        for r in self.w.readers.values(): r.close()
        self.w.db.close(); self.w = Warehouse(self.root)
        s = self.source(path); self.assertEqual(self.w.ingest(s['id']), 1)
        f.write(frame(3)+b'\n'); f.close(); self.w.ingest(s['id'])
        self.assertEqual(self.w.db.execute('SELECT COUNT(*) FROM records').fetchone()[0], 3)
        self.assertEqual(self.w.db.execute('SELECT cursor FROM sources').fetchone()[0], 3)

    def test_same_content_alias_is_not_imported_twice(self):
        p = self.root/'a.jsonl'; q = self.root/'b.jsonl'; p.write_bytes(frame(1)+b'\n'); q.write_bytes(p.read_bytes())
        from marketdb.core import hashed
        kwargs = {'sha256': hashed(p), 'format': 'jsonl'}
        a = self.source(p, **kwargs); self.w.ingest(a['id'])
        b = self.source(q, **kwargs)
        self.assertEqual(a['id'], b['id']); self.assertEqual(self.w.ingest(b['id']),0)
        self.assertEqual(self.w.db.execute('SELECT COUNT(*) FROM aliases').fetchone()[0],2)

    def test_atomic_failure_does_not_advance_cursor(self):
        p=self.root/'fail.jsonl';p.write_bytes(frame(1)+b'\n')
        s=self.source(p,format='jsonl')
        self.w.db.execute("CREATE TRIGGER injected BEFORE INSERT ON ticks BEGIN SELECT RAISE(ABORT,'injected'); END;")
        self.w.db.commit()
        with self.assertRaises(Exception): self.w.ingest(s['id'])
        self.assertEqual(self.w.db.execute('SELECT COUNT(*) FROM records').fetchone()[0],0)
        self.assertEqual(self.w.db.execute('SELECT cursor FROM sources').fetchone()[0],0)

    def test_csv_mode_zero_timezone_and_russian_quotes(self):
        row={'live':0,'trade_id':'B-7','время':'2026-10-20 10:00:00','HL mid':'100','Binance фьюч':'101'}
        src={'format':'csv','timezone_rule':'UTC+02:00','venue_hint':None,'symbol_hint':'BTC','dataset':'tick_window','role':'trade_window','path':'tiki-btc.csv'}
        meta,ticks=decode(json.dumps(row).encode(),src)
        self.assertEqual(meta[-1],'0'); self.assertEqual(meta[0],timestamp('2026-10-20T08:00:00+00:00'))
        self.assertEqual(len(ticks),2); self.assertEqual(ticks[1][11],101)
        self.assertIsNone(timestamp('2026-10-20 10:00:00','unknown'))

    def test_hl_trade_batch_preserves_every_trade(self):
        src={'format':'jsonl','venue_hint':'hl','symbol_hint':None,'dataset':'observer','role':'live'}
        obj={'recv_ms':1792480000100,'data':{'channel':'trades','data':[
            {'coin':'BTC','time':1792480000001,'px':'100','sz':'2'},
            {'coin':'BTC','time':1792480000002,'px':'101','sz':'3'}]}}
        meta,ticks=decode(json.dumps(obj).encode(),src)
        self.assertEqual(len(ticks),2);self.assertEqual([t[3] for t in ticks],[1792480000001,1792480000002])

    def test_spot_depth_without_provider_clock_does_not_invent_timestamp(self):
        src={'format':'jsonl','venue_hint':'binance_spot','symbol_hint':None,'dataset':'public_supplement','role':'live'}
        obj={'recv_ms':1792480000100,'data':{'stream':'btcusdt@depth20@100ms','data':{'lastUpdateId':123,'bids':[['100','2']],'asks':[['102','3']]}}}
        meta,ticks=decode(json.dumps(obj).encode(),src)
        self.assertIsNone(meta[0]);self.assertEqual(ticks[0][0],'binance_spot')
        self.assertEqual(ticks[0][1],'BTC');self.assertIsNone(ticks[0][3]);self.assertEqual(ticks[0][11],101)

    def test_paper_terminal_flag_is_not_the_instrument_symbol(self):
        src={'format':'jsonl','venue_hint':'paper','symbol_hint':None,'dataset':'observer','role':'live'}
        obj={'recv_ms':1792480000100,'data':{'push':{'pub':{'data':[
            {'instrument':'ETH','instrumentId':1,'instrumentTerminal':False,'kind':'close','ts':1792480000001,'exit':2381.05}]}}}}
        meta,ticks=decode(json.dumps(obj).encode(),src)
        self.assertEqual(ticks[0][1],'ETH');self.assertEqual(ticks[0][9],2381.05)


if __name__=='__main__': unittest.main()
