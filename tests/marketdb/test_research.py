import copy
import json
import sqlite3
import tempfile
import unittest
import zlib
from pathlib import Path
from marketdb.core import Warehouse,connect
from marketdb.detector import Detector
from marketdb.evidence import clean,row_phases,save_event
from marketdb.research_schema import init
from marketdb.timeline import available,received

CONFIG=json.loads((Path(__file__).resolve().parents[1]/'configs/lead-lag.json').read_text())


def quote(n,t,venue,mid=100,**extra):
    return {'record_id':n,'sub':0,'source_id':1,'time_ms':t,'available_ms':t,'recv_ms':t,
            'ts_ms':t+100,'venue':venue,'symbol':'BTC','kind':'bbo','bid':mid-.01,'ask':mid+.01,**extra}


class CausalDetectorTests(unittest.TestCase):
    def config(self):
        c=copy.deepcopy(CONFIG);c.update(warmup_ms=0,cooldown_ms=1,threshold_bps=1,
            baseline_tau_ms=100000,max_quote_age_ms=1000,horizons_ms=[100,300],entry_delay_ms=10,exit_delay_ms=10,hedge_delay_ms=5)
        return c

    def sample(self):
        return [quote(1,1000,'hl'),quote(2,1001,'binance_futures'),quote(3,1002,'binance_spot'),
                quote(4,1010,'binance_futures',101),quote(5,1011,'binance_spot',101),
                quote(6,1030,'hl',100.1),quote(7,1200,'binance_futures',101.2),
                quote(8,1400,'hl',100.5),quote(9,1500,'binance_futures',101.1)]

    def test_same_engine_prefix_is_invariant_to_future_and_restart(self):
        config=self.config();a=Detector(config);prefix=[]
        for e in self.sample()[:5]:prefix.extend(a.on(e)[0])
        self.assertTrue(prefix)
        frozen=json.loads(json.dumps(a.state));resumed=Detector(config,frozen)
        remaining_a=[];remaining_b=[]
        for e in self.sample()[5:]:remaining_a.append(a.on(e));remaining_b.append(resumed.on(e))
        self.assertEqual(remaining_a,remaining_b)
        b=Detector(config);other=[]
        altered=self.sample();altered[-1]['bid']=500;altered[-1]['ask']=501
        for e in altered[:5]:other.extend(b.on(e)[0])
        self.assertEqual(prefix,other)

    def test_stale_crossed_and_future_received_quotes_cannot_signal(self):
        c=self.config();d=Detector(c)
        for e in self.sample()[:3]:d.on(e)
        self.assertEqual(d.on(quote(4,5000,'binance_futures',110))[0],[])
        self.assertEqual(d.on(quote(5,5001,'hl',100,bid=101,ask=99))[0],[])
        self.assertEqual(d.state['invalid'],1)
        d.on(quote(6,5002,'hl',100,recv_ms=6000))
        self.assertIsNone(d.quote('BTC','hl',5002))
        d.on(quote(7,4000,'hl'));self.assertEqual(d.state['late'],1)
        d.on(quote(8,5003,'hl',100,ts_ms=1000))
        self.assertIsNone(d.quote('BTC','hl',5003))

    def test_unknown_fees_never_become_zero_or_realized_profit(self):
        d=Detector(self.config());out=[]
        for e in self.sample():out.extend(d.on(e)[1])
        scenarios=[o for o in out if o['status']=='scenario_closed']
        self.assertTrue(scenarios)
        self.assertTrue(all(x['details']['net_pnl_usd'] is None and not x['details']['fees_known'] for x in scenarios))

    def test_mid_history_never_invents_spread_or_fills_and_uses_atomic_snapshots(self):
        c=self.config();c.update(allow_mid_quotes=True,require_spot_confirmation=False)
        d=Detector(c);all_signals=[];out=[]
        for i,t,h,f in [(1,1000,100,100),(2,1020,100,101),(3,1200,100.2,101),(4,1500,100.5,101)]:
            for sub,venue,mid in [(0,'hl',h),(1,'binance_futures',f)]:
                e=quote(i,t,venue,mid);e.update(kind='mid',mid=mid,bid=None,ask=None,recv_ms=None,sub=sub,snapshot_final=sub==1)
                a,b=d.on(e);all_signals.extend(a);out.extend(b)
                if sub==0:self.assertEqual(a,[])
        self.assertTrue(all_signals)
        self.assertIsNone(d.state['quotes']['BTC/hl']['bid'])
        self.assertTrue(out);self.assertFalse(any(x['status']=='scenario_closed' for x in out))
        self.assertTrue(all(p.get('entry') is None for p in d.state['pending']))


class EvidenceTests(unittest.TestCase):
    def test_missing_wallet_row_does_not_create_fill_or_client_send(self):
        row={'id':1,'status':'open','opened_at':1000,'live':1,'position_id':'42',
             'close_state':json.dumps({'decidedAt':1100,'sent':1200})}
        phases=row_phases(row)
        self.assertIn('close_http_ack',[p[0] for p in phases]);self.assertNotIn('http_sent',[p[0] for p in phases])
        self.assertNotIn('exit_filled',[p[0] for p in phases])
        self.assertEqual(clean({'signed':{'r':'secret'},'private_key':'secret','price':100}),{'price':100})

    def test_evidence_is_idempotent_with_exact_raw_amounts_and_modes(self):
        with tempfile.TemporaryDirectory() as folder:
            c=connect(Path(folder)/'test.sqlite');init(c)
            payload={'positionId':'42','kind':'PositionClosed','timestampMs':1792494000000,'priceScaleRaw':'10',
                     'exitPriceRaw':'819185','adjustedPnlRaw':'123456789012345678901','userPaidFeeRaw':'0','paperMintedRaw':'333'}
            obj={'data':{'origin':'paper_account_history','kind':'PositionClosed','payload':payload,'event_ms':1792494000000,'mode':'live','precision':'provider_execution_time'}}
            with c:save_event(c,obj,1792494001000,123);save_event(c,obj,1792494002000,124)
            self.assertEqual(c.execute('SELECT COUNT(*) FROM research_events').fetchone()[0],1)
            self.assertEqual(c.execute('SELECT pnl_raw,fee_raw,price FROM execution_facts').fetchone()[:],('123456789012345678901','0',81918.5))
            c.close()

    def test_availability_timeline_excludes_selected_data_and_preserves_clock(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);w=Warehouse(root)
            for i,role in [(1,'live'),(2,'selected_success')]:
                p=root/(str(i)+'.jsonl');raw={'recv_ms':1792494000000+i,'recv_mono_ns':1234+i,
                    '_capture':{'boot_id':'boot','connection':'c'+str(i),'seq':1},
                    'data':{'stream':'btcusdt@bookTicker','data':{'e':'bookTicker','s':'BTCUSDT','u':9,'b':'100','a':'101'}}}
                p.write_text(json.dumps(raw)+'\n')
                s=w.register({'path':str(p),'format':'jsonl','dataset':'observer','role':role,'venue_hint':'binance_futures','timezone_rule':'epoch'})
                w.ingest(s['id'])
            clk=w.db.execute('SELECT recv_mono_ns,local_seq,provider_seq FROM event_clock').fetchone()
            self.assertEqual(tuple(clk),(1235,1,'9'))
            rows=list(available(w.db,0,1e16));self.assertEqual(len(rows),1)
            self.assertEqual(rows[0]['boot_id'],'boot');self.assertEqual(rows[0]['recv_ms'],1792494000001)
            self.assertEqual(len(list(received(w.db,1792493999000,1792494001000))),1)
            w.db.close()

    def test_available_pagination_never_skips_subtrades_at_batch_boundary(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);w=Warehouse(root);p=root/'hl.jsonl'
            p.write_text(json.dumps({'recv_ms':1792494001000,'data':{'channel':'trades','data':[
                {'coin':'BTC','time':1792494000000+i,'px':'100','sz':'1'} for i in range(3)]}})+'\n')
            s=w.register({'path':str(p),'format':'jsonl','dataset':'observer','role':'live','venue_hint':'hl','timezone_rule':'epoch'});w.ingest(s['id'])
            after=None;seen=[]
            for _ in range(4):
                batch=list(available(w.db,0,1e16,after=after,limit=1))
                if not batch:break
                e=batch[0];seen.append(e['sub']);after=(e['available_ms'],e['record_id'],e['sub'])
            self.assertEqual(seen,[0,1,2]);w.db.close()


if __name__=='__main__':unittest.main()
