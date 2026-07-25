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

# 2026-07-25 (post-user-question "does relayer key do the job on /trades?"):
# Polymarket's CLOB team gated `clob.polymarket.com/trades` behind EIP-712 / L2
# auth — /trades 401's without an HMAC-signed credential (L2 personal API key
# OR an approved relayer key would both work; user asked, I empirically tested).
# The DATA-API service at `data-api.polymarket.com` — separate infra, run by
# Polymarket's analytics team for the user-to-browser dashboard — serves the
# same trade records WITHOUT auth (verified HTTP 200 with `market=<condId>` +
# `limit=N` + `minTimestamp`/`maxTimestamp` Unix-seconds filters).  We're now
# wiring the lab's `_validate_fills_via_trades_truth` path to that endpoint so
# the `+$3.27` lab figures can finally become validated economic PnL rather
# than upper-bound raw signal. Schema differences from CLOB /trades:
#   - `asset` field (was `asset_id`) — renamed in the new endpoint
#   - `timestamp` in Unix SECONDS (CLOB /trades was MS) — we convert on ingress
#   - `takerOnly` flag absent — we treat all entries as maker+taker; the lab's
#     cross-match logic validates by `asset_id + side + ts_window`, which is
#     unaffected by the missing takerOnly flag
#   - `transactionHash` available — used as canonical trade_id
#   - `proxyWallet`/`name`/`pseudonym`/`profileImage`/`eventSlug`/`title`/
#     `icon`/`outcome`/`outcomeIndex`/`slug` populated for the UX dashboard
DEFAULT_CLOB = "https://data-api.polymarket.com"
DEFAULT_TIMEOUT = 8.0
USER_AGENT = "polymarket-lab/0.1 (+local; unwalled data-api truth)"


@dataclass
class TradeRecord:
    asset_id: str
    side: str              # "BUY" | "SELL"
    size: float
    price: float
    ts: int                # ms
    trade_id: str | None = None
    taker_only: bool = True
    fee_rate_bps: float = 0.0


def _fetch_trades(
    market_condition_id: str | None = None,
    asset_id: str | None = None,
    taker_only: bool = True,
    limit: int = 500,
    offset: int = 0,
    timeout: float = DEFAULT_TIMEOUT,
    min_ts_sec: int | None = None,
    max_ts_sec: int | None = None,
) -> list[dict]:
    """Fetch a single page from data-api.polymarket.com/trades.

    data-api supports:  market=<conditionId>  limit=N  offset=N
    minTimestamp=<unixSec>  maxTimestamp=<unixSec>

    `taker_only` is kept for backward-compat with the old CLOB signature but
    is a no-op on data-api (the endpoint doesn't expose a takerOnly filter);
    we treat all returned trades as maker+taker and let the lab's cross-match
    logic discriminate by `side`/`asset_id`/`ts` post-fetch.
    """
    params = {"limit": limit, "offset": offset}
    if market_condition_id:
        params["market"] = market_condition_id
    if asset_id:
        params["asset"] = asset_id
    if min_ts_sec is not None:
        params["minTimestamp"] = int(min_ts_sec)
    if max_ts_sec is not None:
        params["maxTimestamp"] = int(max_ts_sec)
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
    min_ts_ms: int | None = None,
    max_ts_ms: int | None = None,
) -> list[TradeRecord]:
    """Fetch trades for one market_condition_id, paginate, post-filter by asset_id.

    2026-07-25: backend switched from clob.polymarket.com/trades (401 without
    L2 auth) to data-api.polymarket.com/trades (no auth needed). When
    min_ts_ms/max_ts_ms are given, we additionally convert to Unix seconds and
    send as minTimestamp/maxTimestamp — the data-api supports this filter
    directly, avoiding the deep-trades-pagination problem (for high-volume
    markets like BTC daily where 30 pages × 500 = 15k trades can be wall
    depth < 1h; with a time-window filter we get the lab-fill-spanning window
    in a single page).
    """
    out: list[TradeRecord] = []
    seen_ids: set[str] = set()
    min_ts_sec = int(min_ts_ms / 1000.0) if min_ts_ms is not None else None
    max_ts_sec = int(max_ts_ms / 1000.0) if max_ts_ms is not None else None
    for page in range(max_pages):
        offset = page * page_size
        batch = _fetch_trades(
            market_condition_id=market_condition_id,
            asset_id=None,
            taker_only=taker_only,
            limit=page_size,
            offset=offset,
            min_ts_sec=min_ts_sec,
            max_ts_sec=max_ts_sec,
        )
        if not batch:
            break
        for t in batch:
            # data-api schema: `asset` (was `asset_id`), `transactionHash`
            # available as canonical id, `timestamp` in Unix SECONDS (convert
            # to MS to match the lab's fill ts_utc_ms field). CLOB /trades had
            # `id`/`tradeId`/`asset_id`/`takerOnly` — fall through to those
            # aliases for backward-compat with a clob.polymarket.com fetch.
            tid = (
                t.get("transactionHash")
                or t.get("id") or t.get("trade_id")
                or f"{t.get('asset', t.get('asset_id', ''))}-{t.get('timestamp', '')}-{t.get('price', '')}"
            )
            if tid in seen_ids:
                continue
            seen_ids.add(tid)
            tr_asset_id = t.get("asset") or t.get("asset_id") or ""
            if asset_id and tr_asset_id != asset_id:
                continue
            # data-api /trades uses Unix seconds; CLOB /trades used ms. Auto-detect:
            # if the timestamp is < 1e12 it's clearly seconds (1.78 × 10^9 ≈ now).
            raw_ts = t.get("timestamp") or t.get("ts") or 0
            try:
                ts_raw_num = int(raw_ts)
            except (TypeError, ValueError):
                ts_raw_num = 0
            ts_ms = ts_raw_num if ts_raw_num > 10**12 else ts_raw_num * 1000
            try:
                rec = TradeRecord(
                    asset_id=tr_asset_id,
                    side=t.get("side") or "BUY",
                    size=float(t.get("size") or 0.0),
                    price=float(t.get("price") or 0.0),
                    ts=ts_ms,
                    trade_id=str(tid),
                    taker_only=bool(t.get("takerOnly", True)),
                    fee_rate_bps=float(t.get("feeRateBps") or 0.0),
                )
                out.append(rec)
            except Exception:
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
