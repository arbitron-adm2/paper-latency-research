import importlib.util, pathlib, sqlite3, unittest
path=pathlib.Path(__file__).resolve().parents[2]/'apps/strategy-lab/hl-summary-import.py'
spec=importlib.util.spec_from_file_location('hl_summary_import',path);m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
class SummaryImport(unittest.TestCase):
 def row(self):return dict(zip(m.FIELDS,['19.10.2026 00:00:00','1792360800000','81182.5','69','100','5','','','']))
 def test_epoch_and_empty_wall_preserved_without_bbo(self):
  r=m.parse_row(self.row(),4,1);self.assertEqual(r[0],(4<<40)|1);self.assertEqual(r[3],1792360800000);self.assertEqual(r[5],69);self.assertEqual(r[8:],(None,None,None))
  c=sqlite3.connect(':memory:');c.executescript(m.SCHEMA);c.execute('INSERT INTO strategy_hl_book_summaries VALUES(?,?,?,?,?,?,?,?,?,?,?)',r);self.assertEqual(c.execute('SELECT count(*) FROM strategy_hl_book_summaries').fetchone()[0],1);c.close()
 def test_invalid_and_partial_values_refused(self):
  for field,value in [(m.FIELDS[1],'21:00'),(m.FIELDS[2],'nan'),(m.FIELDS[3],'101'),(m.FIELDS[4],'-1'),(m.FIELDS[6],'100')]:
   row=self.row();row[field]=value
   with self.assertRaises(ValueError):m.parse_row(row,4,1)
if __name__=='__main__':unittest.main()
