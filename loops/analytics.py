"""analytics.py — Loop E (Phase 1A): off-line aggregator that produces
daily_summary.parquet from ledger.parquet + candidates_ranked.parquet.

Phase 1A scope (intentionally minimal):
  - Per (date, asset_id) row: fill_count, cycle_count, expected_pnl_sum,
    pnl_worst_sum, pnl_best_sum, markout_60s_mean (where available),
    deployed_cap_peak (reconstructed from inventory walk), last_inventory_shares,
    kill_trigger_fired_count

Phase 1B/Later will:
  - Backfill markout_60s / 5m / 30m by walking raw_events.jsonl — every fill's
    markout is the mid drift between fill_ts_utc and fill_ts_utc + 60s/300s/1800s,
    sampled from raw_events entries with ts_raw >= fill_ts_utc.
  - Build the empirical AS regressor on this ledger.
"""
from __future__ import annotations
import json
import os
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


def _date_iso(ms_or_ts_utc: int | float | None) -> str:
    if not ms_or_ts_utc:
        return ""
    try:
        v = float(ms_or_ts_utc)
        return datetime.fromtimestamp(v / 1000.0, tz=timezone.utc).date().isoformat()
    except (TypeError, ValueError):
        return ""


def aggregate_daily_summary(
    ledger_path: Path,
    daily_summary_path: Path,
) -> int:
    """Return total rows written. Empty ledger produces 0 rows."""
    if not ledger_path.exists():
        return 0
    table = pq.read_table(ledger_path)
    if table.num_rows == 0:
        return 0
    rows = table.to_pylist()
    by_key = defaultdict(dict)
    for r in rows:
        date = _date_iso(r.get("ts_utc"))
        asset = r.get("asset_id") or ""
        if not date:
            continue
        key = (date, asset)
        bucket = by_key[key]
        bucket["fill_count"] = bucket.get("fill_count", 0) + 1
        bucket["expected_pnl_sum"] = bucket.get("expected_pnl_sum", 0.0) + float(r.get("expected_pnl_per_fill") or 0.0)
        bucket["pnl_worst_sum"] = bucket.get("pnl_worst_sum", 0.0) + float(r.get("pnl_worst_case") or 0.0)
        bucket["pnl_best_sum"] = bucket.get("pnl_best_sum", 0.0) + float(r.get("pnl_best_case") or 0.0)
        mk = r.get("markout_60s")
        if isinstance(mk, (int, float)):
            sum0 = bucket.get("markout_60s_sum", 0.0) + mk
            cnt = bucket.get("markout_60s_count", 0) + 1
            bucket["markout_60s_sum"] = sum0
            bucket["markout_60s_count"] = cnt
        ktf = r.get("kill_trigger_fired")
        if ktf:
            bucket["kill_trigger_fired_count"] = bucket.get("kill_trigger_fired_count", 0) + 1
        # market_id, scan_cycle_id stored as latest
        bucket["market_id"] = r.get("market") or ""
        bucket["last_inventory_shares"] = float(r.get("exec_qty", 0.0))  # naive

    out_rows = []
    for (date, asset), v in by_key.items():
        fill_n = v.get("fill_count", 0)
        mk_n = v.get("markout_60s_count", 0)
        out_rows.append({
            "date_utc": date,
            "asset_id": asset,
            "market_id": v.get("market_id", ""),
            "fill_count": fill_n,
            "expected_pnl_sum": v.get("expected_pnl_sum", 0.0),
            "pnl_worst_sum": v.get("pnl_worst_sum", 0.0),
            "pnl_best_sum": v.get("pnl_best_sum", 0.0),
            "markout_60s_mean": ((v.get("markout_60s_sum", 0.0) / mk_n) if mk_n else None),
            "markout_60s_count": mk_n,
            "last_inventory_shares": v.get("last_inventory_shares", 0.0),
            "kill_trigger_fired_count": v.get("kill_trigger_fired_count", 0),
        })
    out_rows.sort(key=lambda r: (r["date_utc"], r["asset_id"]))
    table = pa.Table.from_pylist(out_rows)
    daily_summary_path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, daily_summary_path)
    return len(out_rows)


def backfill_markout_60s_into_ledger(
    ledger_path: Path,
    raw_events_path: Path,
    window_sec: int = 60,
) -> int:
    """Walk raw_events.jsonl, build a (asset_id, ts_raw) -> mid map.

    For each fill in ledger.parquet with markout_60s null, look up the raw events
    around fill_ts_utc + window_sec and compute mid drift.

    Returns the number of rows whose markout_60s was backfilled.
    """
    if not raw_events_path.exists() or not ledger_path.exists():
        return 0
    # Parse raw events once
    walk: list[tuple[int, str, float, float]] = []  # (ts_raw, asset_id, mid, mid_diff)
    bb_a_per_asset: dict[str, float] = {}
    bb_b_per_asset: dict[str, float] = {}
    with open(raw_events_path) as f:
        for line in f:
            if not line.strip():
                continue
            try:
                ev = json.loads(line)
            except Exception:
                continue
            ts = int(ev.get("ts_raw") or ev.get("ts") or 0)
            if ev.get("event_type") == "book":
                aid = ev.get("asset_id")
                bb = ev.get("bids") or []
                ba = ev.get("asks") or []
                try:
                    b = float(bb[0]["price"]) if bb else 0.0
                    a = float(ba[0]["price"]) if ba else 0.0
                except (TypeError, ValueError, KeyError, IndexError):
                    continue
                bb_b_per_asset[aid] = b
                bb_a_per_asset[aid] = a
                mid = (b + a) / 2
                walk.append((ts, aid, mid, 0.0))
            elif ev.get("event_type") == "price_change":
                # Apply all changes for the event, then record the FINAL mid per asset once
                touched = set()
                for c in ev.get("changes") or []:
                    aid = c.get("asset_id")
                    b_b = c.get("best_bid") or bb_b_per_asset.get(aid, 0.0)
                    b_a = c.get("best_ask") or bb_a_per_asset.get(aid, 0.0)
                    try:
                        bb_b_per_asset[aid] = float(b_b)
                        bb_a_per_asset[aid] = float(b_a)
                    except (TypeError, ValueError):
                        continue
                    touched.add(aid)
                for aid in touched:
                    if not aid:
                        continue
                    mid = (bb_b_per_asset[aid] + bb_a_per_asset[aid]) / 2
                    walk.append((ts, aid, mid, 0.0))
    # walk is now a sorted-by-read time sequence; build per-asset time+mid array
    by_asset: dict[str, list[tuple[int, float]]] = defaultdict(list)
    for ts, aid, mid, _ in walk:
        by_asset[aid].append((ts, mid))

    table = pq.read_table(ledger_path)
    rows = table.to_pylist()
    if not rows:
        return 0
    n_filled = 0
    for r in rows:
        if r.get("markout_60s") is not None:
            continue
        aid = r.get("asset_id")
        fill_ts = int(r.get("ts_utc") or 0)
        side_taker = r.get("side_taker")
        qty = float(r.get("exec_qty") or 0.0)
        ev = by_asset.get(aid) or []
        # find mid at fill_ts and mid at fill_ts + window_sec*1000
        m0 = None
        m1 = None
        for ts, mid in ev:
            if ts <= fill_ts:
                m0 = mid
            elif ts <= fill_ts + window_sec * 1000:
                m1 = mid
                break
        if m0 is None or m1 is None:
            continue
        sign = 1.0 if side_taker == "BUY" else -1.0
        r["markout_60s"] = (m1 - m0) * qty * sign
        n_filled += 1
    if n_filled > 0:
        # Write back the updated table
        new_table = pa.Table.from_pylist(rows)
        pq.write_table(new_table, ledger_path)
    return n_filled
