import csv
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

import numpy as np

ROOT=Path(__file__).resolve().parents[2]
SCRATCH=ROOT / 'reports' / 'local-input'
sys.path.insert(0,str(ROOT/'apps/paper-edge-research'))
from engine import payment


class SearchTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        SCRATCH.mkdir(exist_ok=True)
        cls.tmp=tempfile.TemporaryDirectory(dir=SCRATCH,prefix='tests-')
        cls.base=Path(cls.tmp.name);cls.exe=cls.base/'search'
        subprocess.run(['g++','-O2','-std=c++17',str(ROOT/'apps/paper-wr-search/search.cpp'),'-o',str(cls.exe)],check=True,capture_output=True)
        cls.params=json.loads((ROOT/'apps/papertrade-shadow/native-parameters.json').read_text())

    @classmethod
    def tearDownClass(cls):cls.tmp.cleanup()

    def market(self):
        a=np.zeros((1000,8),dtype='<f8');a[:,0]=np.arange(1000);a[:,1:4]=820000
        return a

    def run_case(self,a,risk=0,runs=((300,300,1),),mode='ledger',profile=None,extra=()):
        market=self.base/'market.bin';a.tofile(market);sig=self.base/'signals.bin'
        with sig.open('wb') as f:
            np.array([1,len(runs)],dtype='<i4').tofile(f);np.array(runs,dtype='<i4').tofile(f)
        cases=self.base/'cases.txt';cases.write_text('0 '+str(risk)+'\n');out=self.base/'out.csv'
        command=[str(self.exe),str(market),str(sig),str(out),mode,str(cases),'814.598','.000476041580087574']
        if profile:command.append(profile)
        command.extend(map(str,extra))
        subprocess.run(command,check=True,capture_output=True)
        with out.open() as f:return list(csv.DictReader(f))

    def test_fill_uses_delayed_price_and_actual_close_queue(self):
        a=self.market();a[302:,1:4]=820100;a[306:,1:4]=820200
        r=self.run_case(a)[0]
        self.assertEqual(int(r['entry']),305000);self.assertEqual(int(r['entry_raw']),820100)
        self.assertEqual(int(r['exit']),315000)

    def test_payout_matches_independently_verified_integer_accounting(self):
        a=self.market();a[306:,1:4]=820100
        r=self.run_case(a)[0];expected=payment(2000,550,82000,82010,1,'BTC',self.params)
        self.assertAlmostEqual(float(r['net']),expected['net'],places=5)
        self.assertAlmostEqual(float(r['fee']),expected['fee'],places=5)

    def test_liquidation_while_close_is_queued_loses_full_margin(self):
        a=self.market();a[306:,1:4]=820100;a[310,2]=800000
        r=self.run_case(a)[0]
        self.assertEqual(r['reason'],'liquidation');self.assertEqual(float(r['net']),-2000)
        self.assertEqual(int(r['exit']),310000)

    def test_unknown_path_is_not_a_free_trade(self):
        a=self.market();a[310,4]=7000
        r=self.run_case(a)[0];self.assertEqual(r['unknown'],'1');self.assertEqual(float(r['net']),-2000)

    def test_zero_outcomes_are_in_WR_denominator(self):
        a=self.market();rows=self.run_case(a,mode='cases');s=next(r for r in rows if r['fold']=='0')
        self.assertEqual(s['n'],'1');self.assertEqual(s['zero'],'1');self.assertEqual(float(s['wr']),0)

    def test_bank_is_not_replenished_after_loss(self):
        a=self.market();a[310,2]=800000
        rows=self.run_case(a,runs=((300,800,1),),mode='cases');s=next(r for r in rows if r['fold']=='0')
        self.assertEqual(s['n'],'1');self.assertEqual(float(s['bank']),500);self.assertEqual(s['capital_blocked'],'1')

    def test_one_slot_blocks_overlapping_entries(self):
        a=self.market();rr=self.run_case(a,runs=((300,800,1),))
        for previous,current in zip(rr,rr[1:]):self.assertGreater(int(current['signal']),int(previous['exit']))

    def test_minimum_hold_delays_take_profit_but_not_liquidation(self):
        a=self.market();a[306:,1:4]=820100
        r=self.run_case(a,risk=1,profile='tight')[0]
        self.assertEqual(int(r['exit']),324000)
        a[309,2]=800000;r=self.run_case(a,risk=1,profile='tight')[0]
        self.assertEqual(r['reason'],'liquidation');self.assertEqual(int(r['exit']),309000)

    def test_rolling_median_is_causal(self):
        sys.path.insert(0,str(ROOT/'apps/paper-wr-search'))
        from prepare_pass2 import past_median
        a=np.arange(12,dtype=float);b=a.copy();b[5:]=99999
        self.assertEqual(past_median(a,3)[5],3)
        np.testing.assert_equal(past_median(a,3)[:6],past_median(b,3)[:6])

    def test_custom_exit_grid_controls_hold_and_exit(self):
        grid=self.base/'risks.txt';grid.write_text('.25 6 120 20\n')
        a=self.market();a[306:,1:4]=820100
        r=self.run_case(a,profile='grid:'+str(grid))[0]
        self.assertEqual(int(r['exit']),334000)

    def test_state_exit_waits_for_observed_target_then_close_queue(self):
        grid=self.base/'state-risks.txt';grid.write_text('.25 4 120 0 0\n')
        state=np.full(1000,2.,dtype='<f8');state[330:]=-.1;path=self.base/'state.bin';state.tofile(path)
        a=self.market();a[306:,1:4]=819900
        r=self.run_case(a,runs=((300,300,-1),),profile='grid:'+str(grid),extra=(2000,550,5,9,6000,'-',1,-1,0,path))[0]
        self.assertEqual(r['reason'],'state_target');self.assertEqual(int(r['exit']),339000)

    def test_expired_entry_is_not_counted_as_a_fill_or_win(self):
        pool=self.base/'latency.txt';pool.write_text('66 1 9 2\n')
        rows=self.run_case(self.market(),mode='cases',profile='tight',extra=(2000,550,5,9,6000,pool,1))
        s=next(r for r in rows if r['fold']=='0')
        self.assertEqual(s['expired'],'1');self.assertEqual(s['n'],'0')
        self.assertEqual(float(s['bank']),2500)

    def test_empirical_long_close_queue_reserves_full_outcome_horizon(self):
        pool=self.base/'long-close.txt';pool.write_text('5 1 65 2\n')
        a=self.market()
        rows=self.run_case(a,runs=((900,900,1),),mode='cases',profile='tight',extra=(1500,668,5,18,6000,pool,1))
        # Tight risk0 can hold30s; a signal900 would close1000, beyond last row999.
        s=next(r for r in rows if r['fold']=='0')
        self.assertEqual(s['n'],'0')

    def test_adverse_fill_price_proxy_error_is_applied_to_both_legs(self):
        a=self.market();a[306:,1:4]=820100
        r=self.run_case(a,profile='tight',extra=(2000,550,5,9,6000,'-',1,-1,5))[0]
        self.assertEqual(float(r['entry_raw']),820005)
        self.assertEqual(float(r['exit_raw']),820095)
        expected=payment(2000,550,82000.5,82009.5,1,'BTC',self.params)
        self.assertAlmostEqual(float(r['net']),expected['net'],places=5)

    def test_book_band_excludes_large_levels_outside_two_bps(self):
        sys.path.insert(0,str(ROOT/'apps/paper-wr-search'))
        from align_books import band_imbalance
        result=band_imbalance([[99.99,2],[99,1000000]],[[100.01,1],[101,999999]],100)
        self.assertEqual(result,.33)

    def test_declarative_entry_features_and_signals_are_causal(self):
        sys.path.insert(0,str(ROOT/'apps/paper-wr-search'))
        from causal_entries import features,signal
        n=1600;t=np.arange(n);h=82000+np.sin(t/20)
        z={'hl':h,'bf':h+5+np.sin(t/30)*4,'hl_age':np.zeros(n),
           'bf_age':np.zeros(n),'hl_imb':np.ones(n)*.7,'bf_imb':np.ones(n)*.7}
        changed={k:v.copy() for k,v in z.items()};changed['bf'][1400:]+=200
        config={'minimum_cushion':.1,'maximum_cushion':5,'minimum_book':.4,
                'maximum_range':8,'persistence':3,'side':0,'mode':'quiet'}
        a=signal(features(z),config);b=signal(features(changed),config)
        np.testing.assert_equal(a[:1400],b[:1400])

    def test_ETH_integer_accounting_uses_ETH_parameters_and_precision(self):
        sys.path.insert(0,str(ROOT/'apps/paper-wr-search'))
        from verify import settle
        for side in (-1,1):
            raw,fee=settle(1500,668,241450,241550 if side==1 else 241350,side,self.params,'ETH')
            expected=payment(1500,668,2414.50,2415.50 if side==1 else 2413.50,side,'ETH',self.params)
            self.assertAlmostEqual(raw/1e18,expected['net'],places=7)
            self.assertAlmostEqual(fee/1e18,expected['fee'],places=7)

    def test_directional_signals_cannot_trade_the_other_side_or_read_future(self):
        sys.path.insert(0,str(ROOT/'apps/paper-wr-search'))
        from directional_search import features,signal
        n=1600;t=np.arange(n);h=2400+np.sin(t/20)
        z={'hl':h,'bf':h+.1+np.sin(t/30)*.1,'hl_age':np.zeros(n),
           'bf_age':np.zeros(n),'hl_imb':np.ones(n)*.7,'bf_imb':np.ones(n)*.7}
        changed={k:v.copy() for k,v in z.items()};changed['bf'][1400:]+=200
        for side in (-1,1):
            cfg={'side':side,'family':'reversal','window':120,'threshold':1,
                 'confirm':False,'book':'none','range_max':1000000,'persistence':3}
            a=signal(features(z),cfg);b=signal(features(changed),cfg)
            self.assertTrue(set(np.unique(a)).issubset({0,side}))
            np.testing.assert_equal(a[:1400],b[:1400])

    def test_flow_features_do_not_read_future_trade_volume(self):
        sys.path.insert(0,str(ROOT/'apps/paper-wr-search'))
        from directional_search import features
        n=1600;t=np.arange(n);h=2400+np.sin(t/20)
        z={'hl':h,'bf':h+.1,'hl_age':np.zeros(n),'bf_age':np.zeros(n),
           'hl_imb':np.zeros(n),'bf_imb':np.zeros(n),'flow_buy':np.ones(n)*2,'flow_sell':np.ones(n)}
        other={k:v.copy() for k,v in z.items()};other['flow_sell'][1400:]=9999
        a,b=features(z),features(other)
        for w in (3,10,30):
            np.testing.assert_equal(a['flow'+str(w)][:1400],b['flow'+str(w)][:1400])
            self.assertAlmostEqual(a['flow'+str(w)][500],1/3)


if __name__=='__main__':unittest.main()
