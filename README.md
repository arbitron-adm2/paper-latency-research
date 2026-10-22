# PaperTrade / Hyperliquid / Binance research toolkit

Source-only toolkit for market capture, queue-aware strategy simulation, backtesting and a guarded PaperTrade executor. BTC/ETH, Hyperliquid and Binance spot/futures are supported by the capture modules.

This repository contains no recorded ticks, order books, trade/account histories, databases, wallet bundles, sessions, private keys, Telegram messages, screenshots or historical reports. Test fixtures and queue timing pools are generated examples. The code does not establish a profitable strategy or current execution parity.

## Layout

- `marketdb/`: public WebSocket capture, compression, one SQLite warehouse, clock/coverage and latency analysis.
- `apps/papertrade-shadow/`: strategy registry, execution simulation, checkpointing and guarded source VM.
- `apps/strategy-lab/`: replay, market summaries and strategy comparisons.
- `apps/vchera2/`: dashboard and guarded execution code; local configuration is excluded from Git.
- `apps/paper-edge-research/`, `paper-wr-search/`, `live-parity-backtest/`: research engines. Supply your own local inputs explicitly.
- `apps/paper-queue-latency/`: extension source and timing/feed guards. The unpacked extension uses no bundled images.
- `clients/`: PaperTrade Python client and Binance market client.
- `tests/`: offline synthetic regressions. Data-dependent historical replay fixtures are not distributed.

## Local setup

Python 3.11+ and Node.js 22.13+ (`node:sqlite`) are required. Install the Python requirements in your own virtual environment. The optional PaperTrade and Binance clients have separate dependency manifests. The dashboard/executor dependency is declared in `apps/vchera2/package.json`.

```bash
python -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
cp apps/vchera2/launch-config.example.json apps/vchera2/launch-config.json
# Optional dashboard/executor dependencies:
# npm --prefix apps/vchera2 ci
mkdir -p reports
```

The example configuration has `mode=PAPER`, entries disabled, no wallet, no approvals and an expired cutoff. It does not authorize a live start. Session preparation additionally requires an explicit `PAPER_WALLET` and a separately provided local signer. Secrets and live configuration must stay outside Git.

## Capture your own data

```bash
python tools/init_capture.py
PAPER_CAPTURE_DEPTH=1 PAPER_CAPTURE_PARAMETERS=1 python -m marketdb.observer
# In another terminal, from the same checkout:
PAPER_CAPTURE_DEPTH=1 python -m marketdb.capture
```

`init_capture.py` creates a new explicit recording epoch and refuses to overwrite an existing one. Raw journals use `data/live/`, supplemental Binance feeds use `data/unified/supplemental/`, and the warehouse is `data/unified/market.sqlite3`. None are tracked. No service installation or background processes are started automatically.

The feed contains source and receipt clocks, reconnect evidence, available book snapshots and public Paper trading parameters. It is not an all-order order book, a mempool or proof of counterfactual queue execution. Gaps and unmeasured latency must remain visible in a backtest.

## Validation

```bash
python -m unittest discover -s tests/marketdb -p 'test_capture_epoch.py'
python -m unittest discover -s tests/marketdb -p 'test_ingestion.py'
python -m unittest discover -s tests/marketdb -p 'test_stream_optimization.py'
node --require ./tests/papertrade-shadow/no-network.cjs --test --test-concurrency=2 tests/papertrade-shadow/*.test.cjs
python tools/check_publish.py --history-owner arbitron-adm2
```

The queue presets named `vlad` and `live` retain their API names for compatibility. Their distributed timing arrays are synthetic assumptions, not the original measurements. Recalibrate with your own data before interpreting model PnL. Protocol parameters are versioned examples and must also be refreshed.

Read-only capture, simulation and actual money execution are separate paths. This repository is a research code export, not a deployment or a running trading service.

## Publication identity

File scans do not check Git identity. Before publishing, verify every reachable author and committer with the history-owner check above. Changing a repository owner or adding a mailmap does not remove previous commit identities. The release history must contain only the intended publishing account.
