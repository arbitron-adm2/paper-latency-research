import importlib.util
import json
import sys
import unittest
from pathlib import Path

import numpy as np

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'apps/paper-edge-research'))
from engine import past_mean,persist,features,replay,payment
from extract import View


class ResearchTests(unittest.TestCase):
    def test_past_mean_never_reads_current_or_future(self):
        a=np.arange(8,dtype=float);b=a.copy();b[5:]=99999
        self.assertEqual(past_mean(a,3)[5],3)
        np.testing.assert_equal(past_mean(a,3)[:6],past_mean(b,3)[:6])

    def test_signal_persistence(self):
        np.testing.assert_equal(persist(np.array([1,1,1,-1,-1,-1,0,1,1,1],dtype=np.int8)),[0,0,1,0,0,-1,0,0,0,1])

    def test_small_history_gap_does_not_restart_five_minutes(self):
        x=np.arange(400,dtype=float);x[290]=np.nan
        self.assertTrue(np.isnan(past_mean(x,300)[301]))
        self.assertTrue(np.isfinite(past_mean(x,300,.95)[301]))
        z=self.market();z['hl_age'][290]=2000
        f=features(z);self.assertTrue(f['ready'][301]);self.assertFalse(f['ready'][290])

    def test_event_waits_for_next_boundary(self):
        v=View(1000,5000);v.quote('hl',2999.8,2999,100,102)
        self.assertTrue(np.isnan(v.q['hl'][0,0]));self.assertEqual(v.q['hl'][1,0],2999.8)
        v.put_book('hl',3500,3500,[[200,2]],[[202,3]])
        self.assertTrue(np.isnan(v.q['hl'][2,0])) # Depth cannot refresh quote.

    def test_btc_integer_accounting_parity(self):
        p=json.loads((ROOT/'apps/papertrade-shadow/native-parameters.json').read_text())
        spec=importlib.util.spec_from_file_location('previous_model',ROOT/'apps/live-parity-backtest/model.py');m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
        for side in (1,-1):
            for exit_ in (81000,81998,82000,82000.5,82010,82100):
                self.assertEqual(payment(20,100,82000,exit_,side,'BTC',p),m.cash(20,100,82000,exit_,side==1,p))

    def test_eth_uses_its_price_scale_and_instrument(self):
        p=json.loads((ROOT/'apps/papertrade-shadow/native-parameters.json').read_text())
        v=payment(20,100,2400,2400.01,1,'ETH',p)
        self.assertEqual(v['net'],0)
        self.assertLess(payment(20,100,2400,2399.99,1,'ETH',p)['net'],0)
        self.assertGreater(payment(20,100,2400,2401,1,'ETH',p)['net'],0)

    def market(self,n=400):
        z={'t':np.arange(n,dtype=np.int64)*1000,'gaps':np.zeros(n,dtype=bool)}
        for venue in ('hl','bf','bs'):
            z[venue]=np.full(n,82000.);z[venue+'_age']=np.zeros(n);z[venue+'_low']=z[venue].copy();z[venue+'_high']=z[venue].copy();z[venue+'_imb']=np.zeros(n);z[venue+'_book_age']=np.zeros(n)
        return z

    def test_queue_fill_precedes_observation_and_close_has_queue(self):
        z=self.market();s=np.zeros(400,dtype=np.int8);s[301]=1
        z['hl'][307:]=82100;z['hl_low'][307:]=82100;z['hl_high'][307:]=82100
        p=json.loads((ROOT/'apps/papertrade-shadow/native-parameters.json').read_text())
        summary,rows=replay(z,s,{'hold':30},p,'BTC',300000,399000)
        self.assertEqual(rows[0]['entry_ms'],306000)
        self.assertEqual(rows[0]['exit_ms'],316000)
        self.assertTrue(rows[0]['late']);self.assertEqual(summary['closed'],1)

    def test_liquidation_during_close_queue_and_unknown_not_zero(self):
        p=json.loads((ROOT/'apps/papertrade-shadow/native-parameters.json').read_text());z=self.market();s=np.zeros(400,dtype=np.int8);s[301]=1
        z['hl'][307:]=82100;z['hl_low'][310]=80000
        summary,rows=replay(z,s,{'hold':30},p,'BTC',300000,399000)
        self.assertEqual(rows[0]['reason'],'liquidation');self.assertEqual(rows[0]['net'],-20)
        z=self.market();z['gaps'][310]=True
        summary,rows=replay(z,s,{'hold':30},p,'BTC',300000,399000)
        self.assertEqual(summary['unknown'],1);self.assertEqual(summary['net'],-20)

    def test_one_slot_and_no_topup(self):
        z=self.market();s=np.ones(400,dtype=np.int8);p=json.loads((ROOT/'apps/papertrade-shadow/native-parameters.json').read_text())
        summary,rows=replay(z,s,{'hold':30},p,'BTC',300000,399000)
        self.assertEqual(len(rows),1);self.assertEqual(summary['final_capital'],2300)


if __name__=='__main__':unittest.main()
