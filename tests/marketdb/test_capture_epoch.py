import gzip,json,os,subprocess,sys,tempfile,unittest
from pathlib import Path
from marketdb.capture import CaptureWarehouse
from marketdb.wire import frame

class CaptureEpochTests(unittest.TestCase):
    def test_depth_mode_covers_all_markets_without_changing_default_mode(self):
        env={**os.environ,'PAPER_CAPTURE_DEPTH':'1'}
        p=json.loads(subprocess.check_output([sys.executable,'-c','import json;from marketdb.streams import profile;print(json.dumps(profile()))'],env=env))
        self.assertTrue(p['orderbook_depth'])
        for pair in ['btcusdt','ethusdt']:
            self.assertIn(pair+'@depth20@100ms',p['binance_futures_depth'])
            self.assertNotIn('@depth',p['binance_futures_quotes'])
            self.assertNotIn('@bookTicker',p['binance_futures_depth'])
            self.assertIn(pair+'@depth20@100ms',p['binance_spot'])
        self.assertEqual(len(p['hl_fast']),2)
        self.assertTrue(all(x['subscription']['fast']for x in p['hl_fast']))
        self.assertTrue(all('fast'not in x['subscription']for x in p['hl']['subscriptions']))
    def test_fast_book_is_separate_from_full_book(self):
        from marketdb.core import decode
        payload={'recv_ms':1792671800000,'data':{'channel':'l2Book','data':{'coin':'BTC','time':1792671800000,'levels':[[{'px':'100','sz':'2'}],[{'px':'101','sz':'3'}]]}}}
        for venue,kind in [('hl','depth'),('hl_fast','depth_fast'),('hl_agg','depth_agg')]:
            meta,ticks=decode(json.dumps(payload).encode(),{'format':'gzip','venue_hint':venue,'symbol_hint':None,'dataset':'observer','role':'live'})
            self.assertEqual(ticks[0][0:3],('hl','BTC',kind))
    def test_parameter_versions_survive_restart_and_old_capture_is_excluded(self):
        with tempfile.TemporaryDirectory()as directory:
            root=Path(directory);u=root/'data/unified';u.mkdir(parents=True);live=root/'data/live';live.mkdir()
            (u/'capture-20261022.json').write_text(json.dumps({'start_ms':2000}))
            rows=[]
            for source,stamp in [('old',1000),('paper_parameters',3000)]:
                path=live/(source+'.gz')
                with gzip.open(path,'wb')as f:f.write(frame({'body':{'indexedThroughBlock':42,'exchange':'next','offHours':True,'winFeeRateRaw':'20000000000000000'}},recv_ms=stamp))
                rows.append({'source':source,'path':str(path),'last_recv_ms':stamp,'rows':1})
            (live/'manifest.jsonl').write_text(''.join(json.dumps(r)+'\n'for r in rows))
            for attempt in range(2):
                w=CaptureWarehouse(root)
                try:
                    w.discover()
                    for sid in list(w.history):w.ingest(sid)
                    self.assertEqual(w.db.execute('SELECT COUNT(*) FROM records').fetchone()[0],1)
                    self.assertEqual(tuple(w.db.execute('SELECT indexed_block,exchange_version,off_hours,win_fee_raw FROM capture_paper_parameters').fetchone()),(42,'next',1,'20000000000000000'))
                finally:
                    for reader in w.readers.values():reader.close()
                    w.db.close()
