import importlib.util,pathlib,unittest
p=pathlib.Path(__file__).resolve().parents[2]/'apps/strategy-lab/quote-coalesce.py';s=importlib.util.spec_from_file_location('coalesce',p);m=importlib.util.module_from_spec(s);s.loader.exec_module(m)
def event(t,v='bf',kind='market',frame='quote',source=None):return(t,t,{'kind':kind,'venue':v,'source_ms':source if source is not None else t,'payload':{'frameKind':frame}})
class Quotes(unittest.TestCase):
 def test_preserves_hl_every_book_and_summary_and_last_binance(self):
  x=[event(1),event(2,'hl'),event(3),event(4,'bf',frame='book'),event(5,'hl','book_summary','book_summary'),event(6,'bs'),event(7,'bs')];self.assertEqual([x[0] for x in m.bucket_events(x)],[2,3,4,5,7])
 def test_out_of_order_source_preserves_all_and_order(self):
  x=[event(1,source=10),event(2,source=9),event(3,source=11)];self.assertEqual(m.bucket_events(x),x)
 def test_equal_source_time_uses_last_arrival(self):self.assertEqual(m.bucket_events([event(1,source=1),event(2,source=1)]),[event(2,source=1)])
if __name__=='__main__':unittest.main()
