import json
import sqlite3
import tempfile
import unittest
from unittest import mock
from pathlib import Path

from marketdb.core import connect
from marketdb.assets import init_assets, store_file, restore_file, import_sqlite
from marketdb.bundle import archive, walk


class AssetRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.root=Path(self.tmp.name)
        (self.root/'data/unified').mkdir(parents=True)
        self.c=connect(self.root/'data/unified/market.sqlite3');init_assets(self.c)

    def tearDown(self):
        self.c.close();self.tmp.cleanup()

    def test_lossless_multichunk_restore_dedup_versions_and_archive(self):
        # More than one chunk, including binary bytes; both aliases must recover exactly.
        raw=bytes(range(256))*20000
        p=self.root/'capture.bin';p.write_bytes(raw)
        self.assertTrue(store_file(self.c,'original/capture.bin',p,'test'))
        self.assertTrue(store_file(self.c,'alias/capture.bin',p,'test'))
        self.assertFalse(store_file(self.c,'alias/capture.bin',p,'test'))
        self.assertEqual(self.c.execute('SELECT COUNT(*) FROM assets').fetchone()[0],1)
        self.assertEqual(self.c.execute('SELECT chunks FROM assets').fetchone()[0],2)
        restored=self.root/'restored.bin'
        restore_file(self.c,'original/capture.bin',restored)
        self.assertEqual(restored.read_bytes(),raw)
        with self.assertRaises(FileExistsError):restore_file(self.c,'original/capture.bin',restored)
        report=archive(self.root)
        self.assertTrue(report['complete']);self.assertEqual(report['files_verified'],2)
        self.assertTrue(p.exists());self.assertFalse(report['source_deletion'])
        p.write_bytes(b'new version')
        store_file(self.c,'original/capture.bin',p,'test')
        self.assertEqual(self.c.execute('SELECT COUNT(*) FROM file_versions WHERE path=?',('original/capture.bin',)).fetchone()[0],2)

    def test_native_types_modes_empty_tables_and_live_projection(self):
        p=self.root/'source.sqlite';src=sqlite3.connect(p)
        src.executescript('CREATE TABLE lab_trades(id INTEGER,live INTEGER,price REAL,note TEXT,payload BLOB,password TEXT);CREATE TABLE empty_table(id INTEGER);')
        src.execute('INSERT INTO lab_trades VALUES(?,?,?,?,?,?)',(7,0,81918.5,'тест',b'\x00\xff','never-copy'))
        src.commit();src.close()
        store_file(self.c,'original/source.sqlite',p,'test')
        result=import_sqlite(self.c,p,'vlad')
        self.assertEqual(len(result),2)
        row=self.c.execute('SELECT id,live,price,note,payload,password FROM vlad__lab_trades').fetchone()
        self.assertEqual(tuple(row),(7,0,81918.5,'тест',b'\x00\xff',None))
        self.assertEqual(import_sqlite(self.c,p,'vlad'),[])
        self.assertEqual(self.c.execute('SELECT COUNT(*) FROM vlad__lab_trades').fetchone()[0],1)
        self.assertEqual(self.c.execute('SELECT COUNT(*) FROM native_checks WHERE expected_sha256=actual_sha256').fetchone()[0],2)
        import_sqlite(self.c,p,'bot',live=True)
        src=sqlite3.connect(p);src.execute('UPDATE lab_trades SET price=82000.5');src.commit();src.close()
        import_sqlite(self.c,p,'bot',live=True)
        self.assertEqual(self.c.execute('SELECT price FROM bot__lab_trades').fetchone()[0],82000.5)
        self.assertEqual(self.c.execute('SELECT COUNT(*) FROM bot__lab_trades').fetchone()[0],1)
        self.assertEqual(self.c.execute('SELECT price FROM vlad__lab_trades').fetchone()[0],81918.5)

    def test_operational_private_files_are_excluded(self):
        for name in ['.runtime/session-key.json','wallets/a.json','node_modules/a.js','credentials.json','.env','session_key.json','.git','client.py']:
            p=self.root/name;p.parent.mkdir(exist_ok=True);p.write_text('placeholder')
        self.assertEqual([p.name for p in walk(self.root) if p.name!='market.sqlite3' and not p.name.startswith('market.sqlite3-')],['client.py'])

    def test_mutating_upload_leaves_no_orphan_chunks(self):
        import zlib
        p=self.root/'changing.bin';p.write_bytes(b'old data'*100)
        compress=zlib.compress
        def mutate_then_compress(data, level):
            p.write_bytes(b'new data with another size')
            return compress(data,level)
        with mock.patch('marketdb.assets.zlib.compress',side_effect=mutate_then_compress):
            with self.assertRaisesRegex(RuntimeError,'Source changed during asset snapshot'):
                store_file(self.c,'changing.bin',p,'test')
        for table in ('assets','asset_chunks','files','file_versions'):
            self.assertEqual(self.c.execute('SELECT COUNT(*) FROM '+table).fetchone()[0],0,table)

    def test_runtime_databases_and_sidecars_are_not_archived(self):
        for name in ['reports/persistent-shadow-state/shadow-history.sqlite',
                     'reports/persistent-shadow-state/telemetry.json',
                     'other/active.sqlite3','other/active.sqlite3-wal',
                     'other/old.db-journal','incoming/vlad/immutable.db']:
            p=self.root/name;p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(b'fixture')
        names={str(p.relative_to(self.root)) for p in walk(self.root)}
        self.assertIn('incoming/vlad/immutable.db',names)
        self.assertNotIn('reports/persistent-shadow-state/shadow-history.sqlite',names)
        self.assertNotIn('reports/persistent-shadow-state/telemetry.json',names)
        self.assertNotIn('other/active.sqlite3',names)
        self.assertNotIn('other/active.sqlite3-wal',names)
        self.assertNotIn('other/old.db-journal',names)


if __name__=='__main__':unittest.main()
