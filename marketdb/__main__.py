import argparse
import json
from pathlib import Path
from .core import DEFAULT_ROOT, Warehouse, connect

p = argparse.ArgumentParser(description='Unified Paper/HL/Binance local database')
p.add_argument('--project-root', type=Path, default=DEFAULT_ROOT)
p.add_argument('--database', type=Path)
sub = p.add_subparsers(dest='command', required=True)
s = sub.add_parser('serve'); s.add_argument('--once', action='store_true')
sub.add_parser('status')
q = sub.add_parser('query'); q.add_argument('sql')
a = p.parse_args()
database = a.database or a.project_root / 'data/unified/market.sqlite3'
if a.command == 'serve':
    Warehouse(a.project_root, database).serve(a.once)
else:
    c = connect(database, readonly=True)
    sql = a.sql if a.command == 'query' else 'SELECT * FROM source_coverage'
    # Even PRAGMA/writable schema bypasses are prevented by the read-only connection.
    for row in c.execute(sql): print(json.dumps(dict(row), ensure_ascii=False))
    c.close()
