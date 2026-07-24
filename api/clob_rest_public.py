"""clob_rest_public.py — Polymarket CLOB public REST wrappers (no auth).

Endpoints:
  - GET /book?token_id=X       -> snapshot book {bids, asks, hash, asset_id, ...}
  - GET /trades?takerOnly=...  -> recent trades (warn: ignores asset_id filter; post-filter by asset)
  - GET /prices-history?market=X&interval=1h&fidelity=1  -> LTP series (clamped to ~3000 pts)

All read-only, no L1/L2 signing required.
"""
from __future__ import annotations
import json
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any

DEFAULT_CLOB = "https://clob.polymarket.com"
DEFAULT_TIMEOUT = 6.0
USER_AGENT = "polymarket-maker-phase0/0.1 (+local)"


@dataclass
class ClobRestClient:
    base_url: str = DEFAULT_CLOB
    timeout_sec: float = DEFAULT_TIMEOUT

    def _get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        qs = ""
        if params:
            qs = "?" + urllib.parse.urlencode(
                {k: v for k, v in params.items() if v is not None}, doseq=True
            )
        url = f"{self.base_url}{path}{qs}"
        req = urllib.request.Request(
            url, headers={"User-Agent": USER_AGENT, "Accept": "application/json"}
        )
        with urllib.request.urlopen(req, timeout=self.timeout_sec) as r:
            return json.loads(r.read())

    def fetch_book(self, token_id: str) -> dict | None:
        try:
            return self._get("/book", {"token_id": token_id})
        except Exception:
            return None

    def fetch_trades(
        self,
        market: str | None = None,
        asset_id: str | None = None,
        taker_only: bool | None = None,
        limit: int = 500,
        offset: int = 0,
    ) -> dict | None:
        params = {"limit": limit, "offset": offset}
        if market is not None:
            params["market"] = market
        if asset_id is not None:
            params["asset_id"] = asset_id
        if taker_only is not None:
            params["takerOnly"] = "true" if taker_only else "false"
        try:
            return self._get("/trades", params)
        except Exception:
            return None

    def fetch_prices_history(
        self,
        market: str,
        interval: str = "1h",
        fidelity: int = 1,
        ts_start_ms: int | None = None,
        ts_end_ms: int | None = None,
    ) -> dict | None:
        params = {"market": market}
        if interval is not None:
            params["interval"] = interval
        if fidelity is not None:
            params["fidelity"] = fidelity
        if ts_start_ms is not None:
            params["ts"] = ts_start_ms
        if ts_end_ms is not None:
            params["to"] = ts_end_ms
        try:
            return self._get("/prices-history", params)
        except Exception:
            return None


def _to_float(v: Any, default: float = 0.0) -> float:
    try:
        if v is None:
            return default
        return float(v)
    except Exception:
        return default


def normalize_book(book_msg: dict) -> dict:
    def to_levels(arr):
        out = []
        for entry in arr or []:
            try:
                out.append(
                    {"price": float(entry["price"]), "size": float(entry["size"])}
                )
            except Exception:
                continue
        return out

    return {
        "asset_id": book_msg.get("asset_id"),
        "market": book_msg.get("market"),
        "hash": book_msg.get("hash"),
        "tick_size": _to_float(book_msg.get("tick_size")),
        "last_trade_price": _to_float(book_msg.get("last_trade_price")),
        "bids": to_levels(book_msg.get("bids")),
        "asks": to_levels(book_msg.get("asks")),
        "timestamp": int(book_msg.get("timestamp") or 0),
    }
