"""
Live wallet snapshot via the SSE stream  GET /state/user/live?wallet=<addr>.

The first `wallet` event contains the authoritative account state: balances,
registered session keys, PAPER balance/stake and the list of OPEN positions
(with positionId needed for Close intents). Verified against production.
"""
from __future__ import annotations

import json
import threading
import time
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Optional

import httpx

BASE = "https://exchange.papertrade.xyz"
E18 = Decimal(10) ** 18


@dataclass
class OpenPosition:
    position_id: int
    instrument_id: int
    is_long: bool
    entry_price_raw: int
    margin: Decimal
    leverage: int
    bust_price_raw: int
    opened_at_ms: int
    intent_id: Optional[str]


@dataclass
class PendingOp:
    """An accepted-but-not-yet-executed intent sitting in the relayer queue."""
    intent_id: str
    action: str                  # openPosition | closePositions | paperStake | paperClaim | withdrawToCore | ...
    accepted_at_ms: int
    deadline: int                # raw server value (signed deadline)
    margin: Decimal = Decimal(0)
    is_long: Optional[bool] = None
    close_ids: tuple[int, ...] = ()


@dataclass
class WalletSnapshot:
    address: str
    balance: Decimal            # total collateral (USDC)
    available: Decimal          # free collateral
    locked: Decimal             # margin locked in positions
    paper_balance: Decimal      # unstaked PAPER in the exchange
    paper_staked: Decimal
    session_keys: list[tuple[str, int]] = field(default_factory=list)
    open_positions: list[OpenPosition] = field(default_factory=list)
    pending_ops: list[PendingOp] = field(default_factory=list)
    queue_revision: str = ""
    raw: dict = field(default_factory=dict)

    def pending(self, *actions: str) -> list[PendingOp]:
        return [o for o in self.pending_ops if not actions or o.action in actions]

    def has_session_key(self, addr: str) -> bool:
        now = time.time()
        return any(k.lower() == addr.lower() and int(exp) > now for k, exp in self.session_keys)


def _parse(d: dict) -> WalletSnapshot:
    pos = [
        OpenPosition(
            position_id=int(p[0]), instrument_id=int(p[1]), is_long=bool(p[2]),
            entry_price_raw=int(p[4]), margin=Decimal(p[5]) / E18, leverage=int(p[6]),
            bust_price_raw=int(p[7]), opened_at_ms=int(p[8]), intent_id=p[9],
        )
        for p in d.get("openPositions", [])
    ]
    ops = []
    for o in d.get("operations", []) or []:
        # tuple: [intentId, action, instrumentId, isLong, useDebt, margin, leverage, acceptedAtMs, deadline, closeIds]
        try:
            ops.append(PendingOp(
                intent_id=o[0], action=o[1], accepted_at_ms=int(o[7]), deadline=int(o[8]),
                margin=Decimal(o[5]) / E18 if o[5] else Decimal(0),
                is_long=o[3], close_ids=tuple(int(x) for x in (o[9] or [])),
            ))
        except (IndexError, TypeError, ValueError):
            continue
    funds = d.get("funds", {})
    paper = d.get("paper", {})
    return WalletSnapshot(
        address=d["address"],
        balance=Decimal(d.get("balance", "0")) / E18,
        available=Decimal(funds.get("availableBalance", "0")) / E18,
        locked=Decimal(d.get("locked", "0")) / E18,
        paper_balance=Decimal(paper.get("balance", "0")) / E18,
        paper_staked=Decimal(paper.get("staked", "0")) / E18,
        session_keys=[(k, int(e)) for k, e in d.get("sessionKeys", [])],
        open_positions=pos,
        pending_ops=ops,
        queue_revision=str(d.get("queueRevision", "")),
        raw=d,
    )


# The exchange sits behind Cloudflare: ~20 requests in ~13s => HTTP 429 with `Retry-After: 60`.
# One throttle per process + a short cache + honouring Retry-After keeps the farmer out of that ban.
_rl_lock = threading.Lock()
_next_ok = 0.0
MIN_INTERVAL_S = 0.8
CACHE_TTL_S = 1.5
MAX_RETRY_AFTER_S = 90.0
_cache: dict[str, tuple[float, "WalletSnapshot"]] = {}


def _wait_slot() -> None:
    global _next_ok
    with _rl_lock:
        now = time.time()
        wait = max(0.0, _next_ok - now)
        _next_ok = max(now, _next_ok) + MIN_INTERVAL_S
    if wait:
        time.sleep(wait)


def _penalize(seconds: float) -> None:
    global _next_ok
    with _rl_lock:
        _next_ok = max(_next_ok, time.time() + min(seconds, MAX_RETRY_AFTER_S))


def get_snapshot(address: str, timeout_s: float = 15.0, retries: int = 3) -> WalletSnapshot:
    """Read the first `wallet` SSE event and return a parsed snapshot (throttled, cached ~1.5s, 429-aware)."""
    key = address.lower()
    hit = _cache.get(key)
    if hit and time.time() - hit[0] < CACHE_TTL_S:
        return hit[1]
    last: Exception | None = None
    for attempt in range(retries):
        _wait_slot()
        try:
            with httpx.stream(
                "GET", f"{BASE}/state/user/live", params={"wallet": key},
                headers={"Accept": "text/event-stream"}, timeout=timeout_s,
            ) as r:
                if r.status_code == 429:
                    try:
                        ra = float(r.headers.get("retry-after", 30))
                    except ValueError:
                        ra = 30.0
                    _penalize(ra)
                    last = RuntimeError(f"HTTP 429 rate limited (retry-after {ra:.0f}s)")
                    continue
                r.raise_for_status()
                ev = None
                for line in r.iter_lines():
                    if line.startswith("event: "):
                        ev = line[7:].strip()
                    elif line.startswith("data: ") and ev == "wallet":
                        snap = _parse(json.loads(line[6:]))
                        _cache[key] = (time.time(), snap)
                        return snap
            raise RuntimeError("stream ended without wallet event")
        except Exception as exc:  # noqa: BLE001
            last = exc
            time.sleep(2 * (attempt + 1))
    raise RuntimeError(f"live snapshot failed for {address}: {last}")


# ── relayer queue visibility ─────────────────────────────────────────────

def get_queue_rank(address: str, timeout_s: float = 8.0) -> dict[str, int]:
    """intentId -> position in the execution queue (1 = next). Empty dict if nothing queued."""
    r = httpx.get(f"{BASE}/queue-rank/accounts/{address.lower()}", timeout=timeout_s)
    r.raise_for_status()
    return {e["intentId"]: int(e["position"]) for e in r.json().get("entries", [])}


STREAM_WS = "wss://stream.papertrade.xyz/connection/websocket"
_pressure_cache: tuple[float, dict] | None = None


def get_pressure(max_age_s: float = 4.0, timeout_s: float = 8.0) -> Optional[dict]:
    """
    Relayer load snapshot from Centrifugo channel `pressure:current`:
    saturationPct, queuedIntents, submittedIntents, totalPendingGas, blockGasLimit, ts.
    Returns None if the stream is unreachable (caller decides fail-open / fail-closed).
    """
    global _pressure_cache
    now = time.time()
    if _pressure_cache and now - _pressure_cache[0] < max_age_s:
        return _pressure_cache[1]
    import asyncio
    import websockets

    async def _fetch() -> Optional[dict]:
        async with websockets.connect(
            STREAM_WS, origin=BASE, open_timeout=timeout_s, max_size=2**22
        ) as ws:
            await ws.send(json.dumps({"id": 1, "connect": {"name": "js"}}))
            await ws.send(json.dumps({"id": 2, "subscribe": {"channel": "pressure:current"}}))
            deadline = time.time() + timeout_s
            while time.time() < deadline:
                msg = await asyncio.wait_for(ws.recv(), timeout_s)
                for line in msg.split("\n"):
                    if not line.strip():
                        continue
                    d = json.loads(line)
                    if d == {}:
                        await ws.send("{}")      # centrifugo ping -> pong
                        continue
                    pubs = d.get("subscribe", {}).get("publications") or []
                    if pubs:
                        return pubs[-1]["data"]["snapshot"]
                    if "push" in d and "pub" in d["push"]:
                        return d["push"]["pub"]["data"]["snapshot"]
        return None

    try:
        snap = asyncio.run(_fetch())
    except Exception:  # noqa: BLE001
        return None
    if snap:
        snap = dict(snap)
        snap["_fetched_at"] = now
        _pressure_cache = (now, snap)
    return snap


def estimated_wait_s(pressure: dict, block_time_s: float = 1.0) -> float:
    """Rough time to drain the queue: pending gas / gas per block * block time."""
    try:
        return max(1.0, int(pressure["totalPendingGas"]) / max(1, int(pressure["blockGasLimit"])) * block_time_s)
    except (KeyError, ValueError, TypeError):
        return 0.0


PRICE_SCALE = {0: 10, 1: 100}        # raw exchange price / scale = USD (instrument id: 0 BTC, 1 ETH)
SYMBOL = {0: "BTC", 1: "ETH"}


def get_mid(symbol: str, timeout_s: float = 6.0) -> Optional[float]:
    """Hyperliquid mid price (market data host, separate from the exchange's Cloudflare limit)."""
    try:
        r = httpx.post("https://api.hyperliquid.xyz/info", json={"type": "allMids"}, timeout=timeout_s)
        return float(r.json()[symbol])
    except Exception:  # noqa: BLE001
        return None


def scalp_trigger(entry: float, price: float, bust_long: float, bust_short: float, fraction: float) -> Optional[str]:
    """'up' / 'down' once price has travelled `fraction` of the way from entry to the nearest leg's liquidation."""
    if fraction <= 0:
        return None
    if price >= entry + fraction * (bust_short - entry):
        return "up"
    if price <= entry - fraction * (entry - bust_long):
        return "down"
    return None
