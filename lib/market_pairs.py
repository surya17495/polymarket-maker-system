"""market_pairs.py — one-shot gamma-API market pair map builder.

Builds dict[asset_id -> MarketPair] from gamma /events response with YES+NO
token ids mapped per condition_id. Used by lib/mirrored_book.py and
lib/strategies.py to know which NO token to derive/mirror from a YES token.

Cached locally at state/market_pairs.parquet (skipped on subsequent lab runs);
refreshable via `--refresh` flag.
"""
from __future__ import annotations
import json
import logging
import urllib.parse
import urllib.request
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq


log = logging.getLogger(__name__)


@dataclass(frozen=True)
class MarketPair:
    condition_id: str
    yes_token_id: str
    no_token_id: str
    event_slug: str = ""
    market_question: str = ""
    end_date: str | None = None
    neg_risk: bool = False
    fee_type: str = ""

    @property
    def pair(self) -> tuple[str, str]:
        return (self.yes_token_id, self.no_token_id)


DEFAULT_GAMMA = "https://gamma-api.polymarket.com/events"
DEFAULT_TIMEOUT = 8.0
DEFAULT_HEADERS = {"User-Agent": "polymarket-lab/0.1 (+local)", "Accept": "application/json"}


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


def _fetch_events(max_events: int = 2500, page_size: int = 100, max_pages: int = 25) -> list[dict]:
    """Synchronous gamma fetch, paginated."""
    base = DEFAULT_GAMMA
    all_events: list[dict] = []
    for page in range(max_pages):
        offset = page * page_size
        params = {
            "limit": page_size,
            "offset": offset,
            "active": "true",
            "closed": "false",
            "order": "volume24hr",
            "ascending": "false",
        }
        qs = urllib.parse.urlencode(params)
        url = f"{base}?{qs}"
        try:
            req = urllib.request.Request(url, headers=DEFAULT_HEADERS)
            with urllib.request.urlopen(req, timeout=DEFAULT_TIMEOUT) as r:
                batch = json.loads(r.read())
        except Exception:
            break
        if not batch:
            break
        all_events.extend(batch)
        if len(batch) < page_size or len(all_events) >= max_events:
            break
    return all_events[:max_events]


def build_pair_map(max_events: int = 2500) -> dict[str, MarketPair]:
    """One-shot universe fetch; one dict entry per active token_id (both YES and NO)."""
    events = _fetch_events(max_events=max_events)
    pair_map: dict[str, MarketPair] = {}
    for ev in events:
        neg_risk = bool(ev.get("negRisk") or False)
        slug = ev.get("slug") or ""
        end_date = ev.get("endDate")
        for m in ev.get("markets", []):
            cid = m.get("conditionId")
            tokens = _parse_clob_token_ids(m.get("clobTokenIds"))
            if not cid or len(tokens) < 2:
                continue
            pair = MarketPair(
                condition_id=cid,
                yes_token_id=tokens[0],
                no_token_id=tokens[1],
                event_slug=slug,
                market_question=m.get("question", ""),
                end_date=m.get("endDate") or end_date,
                neg_risk=neg_risk,
                fee_type=m.get("feeType") or "default",
            )
            pair_map[pair.yes_token_id] = pair
            pair_map[pair.no_token_id] = pair
    return pair_map


def save_pair_map(pair_map: dict[str, MarketPair], path: Path) -> None:
    rows = []
    seen = set()
    for asset_id, pair in pair_map.items():
        # Save one row per MarketPair (deduplicate YES/NO entries)
        if pair.condition_id in seen:
            continue
        seen.add(pair.condition_id)
        rows.append(asdict(pair))
    if not rows:
        return
    table = pa.Table.from_pylist(rows)
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, path)
    log.info("saved %d market pairs to %s", len(rows), path)


def load_pair_map(path: Path) -> dict[str, MarketPair]:
    if not path.exists():
        return {}
    table = pq.read_table(path).to_pylist()
    pair_map: dict[str, MarketPair] = {}
    for row in table:
        try:
            pair = MarketPair(
                condition_id=row["condition_id"],
                yes_token_id=row["yes_token_id"],
                no_token_id=row["no_token_id"],
                event_slug=row.get("event_slug", ""),
                market_question=row.get("market_question", ""),
                end_date=row.get("end_date"),
                neg_risk=bool(row.get("neg_risk", False)),
                fee_type=row.get("fee_type", ""),
            )
            pair_map[pair.yes_token_id] = pair
            pair_map[pair.no_token_id] = pair
        except Exception:
            continue
    return pair_map


def build_or_load_pair_map(cache_path: Path, refresh: bool = False, max_events: int = 2500) -> dict[str, MarketPair]:
    if cache_path.exists() and not refresh:
        log.info("loading cached pair map from %s", cache_path)
        return load_pair_map(cache_path)
    log.info("fetching fresh pair map from gamma-api...")
    pair_map = build_pair_map(max_events=max_events)
    save_pair_map(pair_map, cache_path)
    return pair_map


__all__ = ["MarketPair", "build_pair_map", "load_pair_map", "save_pair_map", "build_or_load_pair_map"]
