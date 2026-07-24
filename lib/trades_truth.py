"""trades_truth.py — /trades authoritative fill-side label fetcher.

arxiv 2604.24366 found trade-direction inferred from the public order-book feed
agrees with on-chain ground truth only 59–62% of the time. Polymarket's /trades
endpoint supplies authoritative `side` and taker/maker flags per trade; this
module fetches trades per market and post-filters by `asset_id` (since /trades
ignores the asset_id filter when no market filter is given).

Phase 1A Strategy Lab uses this for AS_regressor ground truth (Contract 5):
  - For each premise (asset_id, ts_raw) pair: find trades in the time window.
  - Aggregate taker-side volume BUY vs SELL → signed flow strength.
"""
from __future__ import annotations
import json
import logging
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

log = logging.getLogger(__name__)

DEFAULT_CLOB = "https://clob.polymarket.com"
DEFAULT_TIMEOUT = 8.0
USER_AGENT = "polymarket-lab/0.1 (+local)"


@dataclass
class TradeRecord:
    asset_id: str
    side: str              # "BUY" | "SELL"
    size: float
    price: float
    ts: int                # ms
    trade_id: str | None = None
    takerOnly: bool = True
    fee_rate_bps: float = 0.0


def _fetch_trades(
    market_condition_id: str | None = None,
    asset_id: str | None = None,
    taker_only: bool = True,
    limit: int = 500,
    offset: int = 0,
    timeout: float = DEFAULT_TIMEOUT,
) -> list[dict]:
    """Paginated /trades fetch.

    Per brief: /trades ignores asset_id filter when no market filter is given;
    so we filter by market_condition_id first (when known), then post-filter by asset_id.
    """
    params = {"limit": limit, "offset": offset}
    if market_condition_id:
        params["market"] = market_condition_id
    if asset_id:
        params["asset_id"] = asset_id
    if taker_only:
        params["takerOnly"] = "true"
    qs = urllib.parse.urlencode(params)
    url = f"{DEFAULT_CLOB}/trades?{qs}"
    req = urllib.request.Request(
        url, headers={"User-Agent": USER_AGENT, "Accept": "application/json"}
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            body = r.read()
        return json.loads(body) or []
    except Exception as exc:
        log.warning("trades fetch failed (%s): %s", url, exc)
        return []


def fetch_all_trades(
    market_condition_id: str,
    asset_id: str | None = None,
    taker_only: bool = True,
    max_pages: int = 30,
    page_size: int = 500,
) -> list[TradeRecord]:
    """Fetch trades for one market_condition_id, paginate, post-filter by asset_id."""
    out: list[TradeRecord] = []
    seen_ids: set[str] = set()
    for page in range(max_pages):
        offset = page * page_size
        batch = _fetch_trades(
            market_condition_id=market_condition_id,
            asset_id=None,
            taker_only=taker_only,
            limit=page_size,
            offset=offset,
        )
        if not batch:
            break
        for t in batch:
            tid = t.get("id") or t.get("trade_id") or f"{t.get('asset_id', '')}-{t.get('timestamp', '')}-{t.get('price', '')}"
            if tid in seen_ids:
                continue
            seen_ids.add(tid)
            tr_asset_id = t.get("asset_id") or ""
            if asset_id and tr_asset_id != asset_id:
                continue
            try:
                rec = TradeRecord(
                    asset_id=tr_asset_id,
                    side=t.get("side") or "BUY",
                    size=float(t.get("size") or 0.0),
                    price=float(t.get("price") or 0.0),
                    ts=int(t.get("timestamp") or t.get("ts") or 0),
                    trade_id=str(tid),
                    taker_only=bool(t.get("takerOnly", True)),
                    fee_rate_bps=float(t.get("feeRateBps") or 0.0),
                )
                out.append(rec)
            except Exception as e:
                continue
        if len(batch) < page_size:
            break
    return out


def save_trades_to_parquet(trades: list[TradeRecord], path: Path) -> int:
    if not trades:
        return 0
    rows = [
        {
            "asset_id": t.asset_id,
            "side": t.side,
            "size": t.size,
            "price": t.price,
            "ts_ms": t.ts,
            "trade_id": t.trade_id,
            "taker_only": t.takerOnly,
            "fee_rate_bps": t.fee_rate_bps,
        }
        for t in trades
    ]
    table = pa.Table.from_pylist(rows)
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, path)
    return len(rows)


def load_trades_from_parquet(path: Path) -> list[TradeRecord]:
    if not path.exists():
        return []
    table = pq.read_table(path)
    rows = table.to_pylist()
    return [
        TradeRecord(
            asset_id=r["asset_id"], side=r["side"], size=r["size"],
            price=r["price"], ts=r["ts_ms"], trade_id=r.get("trade_id"),
            taker_only=r.get("taker_only", True),
            fee_rate_bps=r.get("fee_rate_bps", 0.0),
        )
        for r in rows
    ]


def authoritative_taker_flow(trades: list[TradeRecord], window_start_ms: int, window_end_ms: int) -> dict:
    """Aggregate per-window signed taker flow: BUY volume minus SELL volume, per asset_id."""
    by_asset: dict[str, dict[str, float]] = {}
    for t in trades:
        if not (window_start_ms <= t.ts <= window_end_ms):
            continue
        a = t.asset_id
        if a not in by_asset:
            by_asset[a] = {"signed": 0.0, "abs": 0.0, "n": 0}
        sign = 1.0 if t.side == "BUY" else -1.0
        usd = t.size * t.price
        by_asset[a]["signed"] += sign * usd
        by_asset[a]["abs"] += abs(usd)
        by_asset[a]["n"] += 1
    return by_asset


__all__ = [
    "TradeRecord", "fetch_all_trades", "save_trades_to_parquet",
    "load_trades_from_parquet", "authoritative_taker_flow",
]
