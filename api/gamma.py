"""gamma.py — Polymarket gamma-api /events + /markets wrappers.

Returns normalized dicts for an event and its markets, including:
  - event: title, slug, vol24hr, volNum, liquidity, negRisk, endDate, createdAt
  - per-market: question, clobTokenIds (list[str]), feeType, feesEnabled,
    conditionId, bestBid, bestAsk, lastTradePrice, volume, liquidity

Pagination via ?offset= and ?limit=. Order by volume24hr desc for scanner reuse.
"""
from __future__ import annotations
import json
import urllib.parse
from dataclasses import dataclass, field
from typing import Any, Iterator
import urllib.request

DEFAULT_GAMMA = "https://gamma-api.polymarket.com/events"
DEFAULT_TIMEOUT = 8.0
USER_AGENT = "polymarket-maker-phase0/0.1 (+local)"


@dataclass
class GammaClient:
    base_url: str = DEFAULT_GAMMA
    timeout_sec: float = DEFAULT_TIMEOUT
    page_size: int = 100   # gamma hard-caps at 100 events per request (verified empirically)
    max_pages: int = 25    # 25 * 100 = 2500 events covers the full active universe (~2100 shown by pagination discovery)

    def _get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        qs = ""
        if params:
            cleaned = {k: v for k, v in params.items() if v is not None}
            qs = "?" + urllib.parse.urlencode(cleaned, doseq=True)
        url = f"{self.base_url}{path}{qs}"
        req = urllib.request.Request(
            url, headers={"User-Agent": USER_AGENT, "Accept": "application/json"}
        )
        with urllib.request.urlopen(req, timeout=self.timeout_sec) as resp:
            body = resp.read()
        return json.loads(body)

    def fetch_events(
        self,
        max_events: int | None = None,
        active: bool = True,
        closed: bool = False,
    ) -> list[dict]:
        max_events = max_events or self.page_size * self.max_pages
        all_events: list[dict] = []
        for page in range(self.max_pages):
            offset = page * self.page_size
            params = {
                "limit": self.page_size,
                "offset": offset,
                "active": "true" if active else "false",
                "closed": "true" if closed else "false",
                "order": "volume24hr",
                "ascending": "false",
            }
            try:
                batch = self._get("", params)
            except urllib.request.HTTPError as exc:
                if exc.code == 422:
                    # gamma nudges past the actual universe size (offset beyond last page)
                    break
                raise
            except Exception:
                break
            if not batch:
                break
            all_events.extend(batch)
            if len(batch) < self.page_size or len(all_events) >= max_events:
                break
        return all_events[:max_events]

    def fetch_event(self, event_slug: str) -> dict | None:
        try:
            results = self._get("", {"slug": event_slug, "limit": 1})
            return results[0] if results else None
        except Exception:
            return None


def _parse_clob_token_ids(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(x) for x in value]
    s = str(value).strip()
    if s.startswith("["):
        try:
            return [str(x) for x in json.loads(s)]
        except Exception:
            return []
    if "," in s:
        return [p.strip() for p in s.split(",") if p.strip()]
    return [s] if s else []


def normalize_event(ev: dict) -> dict:
    return {
        "event_id": ev.get("id"),
        "event_slug": ev.get("slug"),
        "event_title": ev.get("title", ""),
        "event_vol24": float(ev.get("volume24hr") or 0),
        "event_vol_num": float(ev.get("volumeNum") or 0),
        "event_liquidity": float(ev.get("liquidity") or 0),
        "event_neg_risk": bool(ev.get("negRisk") or False),
        "event_end_date": ev.get("endDate"),
        "event_start_date": ev.get("startDate") or ev.get("createdAt"),
        "event_markets": ev.get("markets", []),
    }


def normalize_market(m: dict, parent_event: dict | None = None) -> dict:
    return {
        "condition_id": m.get("conditionId") or m.get("condition_id"),
        "question": m.get("question", ""),
        "clob_token_ids": _parse_clob_token_ids(
            m.get("clobTokenIds") or m.get("clob_token_ids")
        ),
        "best_bid": _to_float(m.get("bestBid") or m.get("best_bid")),
        "best_ask": _to_float(m.get("bestAsk") or m.get("best_ask")),
        "last_trade_price": _to_float(m.get("lastTradePrice") or m.get("lastTradePrice")),
        "spread_c": (
            (_to_float(m.get("bestAsk")) - _to_float(m.get("bestBid"))) * 100.0
            if m.get("bestBid") is not None and m.get("bestAsk") is not None
            else None
        ),
        "market_vol": _to_float(m.get("volume") or m.get("volumeNum") or 0),
        "market_liquidity": _to_float(m.get("liquidity") or 0),
        "fee_type": m.get("feeType") or "default",
        "fees_enabled": bool(m.get("feesEnabled") or False),
        "neg_risk": bool(
            (parent_event or {}).get("negRisk", False) if parent_event else False
        ),
        "event_slug": (parent_event or {}).get("slug"),
        "event_title": (parent_event or {}).get("title"),
        "event_vol24": (parent_event or {}).get("volume24hr", 0),
        "end_date": m.get("endDate") or (parent_event or {}).get("endDate"),
    }


def _to_float(v: Any, default: float = 0.0) -> float:
    try:
        if v is None:
            return default
        return float(v)
    except Exception:
        return default


def iter_markets(events: list[dict]) -> Iterator[dict]:
    for ev in events:
        norm_ev = normalize_event(ev)
        for m in ev.get("markets", []):
            yield normalize_market(m, norm_ev)
