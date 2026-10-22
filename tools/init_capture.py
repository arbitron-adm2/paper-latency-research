"""Initialize a new local capture epoch without starting a service."""
import argparse
import json
import time
from pathlib import Path

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--project-root', type=Path, default=Path(__file__).resolve().parents[1])
    args = parser.parse_args()
    folder = args.project_root.resolve() / 'data/unified'
    folder.mkdir(parents=True, exist_ok=True)
    (args.project_root.resolve() / 'data/live').mkdir(parents=True, exist_ok=True)
    with (folder / 'capture-epoch.json').open('x') as f:
        json.dump({'start_ms': time.time() * 1000, 'purpose': 'explicit local capture'}, f)
    print('Initialized local capture epoch; no service started.')

if __name__ == '__main__':
    main()
