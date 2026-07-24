"""compounding_score.py — composite compounding score per strategy.

Goal: pick strategies that COMPOUND capital — not merely positive per-fill PnL but
those where capital recycles fast (merges + sell-to-close round trips), AS-drag
is low, and tail-risk rate is low. Many strategies with positive pnl_worst_case
still saturate inventory and stop deploying capital — they compound slowly.

Composite (per strategy):
  composite = sign(Σ pnl_worst_case > 0)
              × capital_recycling_rate
              × (1 − as_drag_per_fill_avg / gross_per_fill_avg)
              × (1 − tail_rate)
              × log(1 + fill_count)

Where:
  capital_recycling_rate = (merges_emitted + inv_returned_via_asks) / total_fills
  as_drag_per_fill_avg   = mean(markout_60s × sign_per_fill across fills)
  tail_rate              = (count fills with pnl_worst_case < −1.0) / total_fills
  gross_per_fill_avg     = mean(|gross_edge_at_fill|)

All inputs come from the per-strategy ledger_<sid>.parquet + the per-strategy
merges_<sid>.parquet (when applicable).

A composite of 0 indicates no fills; reliabilityBias bars overfilling (log term).
"""
from __future__ import annotations
import math
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq


def compute_composite_score(ledger_path: Path, merges_path: Path | None = None) -> dict[str, float]:
    """Return dict of metrics + the composite score for one strategy's ledger.
    
    Idempotent — does not require both files to exist. Missing merges ⇒ no merge recycling credit.
    """
    if not ledger_path.exists():
        return {
            "fill_count": 0, "pnl_worst_sum": 0.0, "sign_positive": 0.0,
            "capital_recycling_rate": 0.0, "as_drag_per_fill_avg": 0.0,
            "gross_per_fill_avg": 0.0, "tail_rate": 0.0,
            "merges_count": 0, "capital_returned_via_merges_usd": 0.0,
            "composite_score": 0.0,
        }
    table = pq.read_table(ledger_path)
    rows = table.to_pylist()
    if not rows:
        return {
            "fill_count": 0, "pnl_worst_sum": 0.0, "sign_positive": 0.0,
            "capital_recycling_rate": 0.0, "as_drag_per_fill_avg": 0.0,
            "gross_per_fill_avg": 0.0, "tail_rate": 0.0,
            "merges_count": 0, "capital_returned_via_merges_usd": 0.0,
            "composite_score": 0.0,
        }
    n_fills = len(rows)
    pnl_worst_sum = sum(float(r.get("pnl_worst_case") or 0.0) for r in rows)
    sign_positive = 1.0 if pnl_worst_sum > 0 else 0.0
    as_drag_sum = 0.0
    as_drag_n = 0
    gross_per_fill_sum = 0.0
    tail_count = 0
    for r in rows:
        mk = r.get("markout_60s")
        if isinstance(mk, (int, float)) and mk is not None:
            as_drag_sum += float(mk)
            as_drag_n += 1
        gs = r.get("gross_edge_at_fill")
        if gs is not None:
            gross_per_fill_sum += abs(float(gs))
        pwc = float(r.get("pnl_worst_case") or 0.0)
        if pwc < -1.0:
            tail_count += 1
    as_drag_per_fill_avg = (as_drag_sum / as_drag_n) if as_drag_n else 0.0
    gross_per_fill_avg = (gross_per_fill_sum / n_fills) if n_fills else 0.0
    tail_rate = (tail_count / n_fills) if n_fills else 0.0

    merges_count = 0
    capital_returned_via_merges_usd = 0.0
    if merges_path is not None and merges_path.exists():
        try:
            mt = pq.read_table(merges_path).to_pylist()
            for m in mt:
                merges_count += 1
                capital_returned_via_merges_usd += float(m.get("capital_returned_usd") or 0.0)
        except Exception:
            pass

    # Inventory returned via ask-fills: count fills where the taker bought our ASK
    # (inventory wound DOWN). This is the SELL-side exit fills recycling capital.
    inv_returned_via_asks = 0
    for r in rows:
        if r.get("side_taker") == "BUY":
            # taker BOUGHT our ASK → we SOLD inventory we previously bought → capital returns
            inv_returned_via_asks += abs(float(r.get("exec_qty") or 0.0)) * abs(float(r.get("exec_price") or 0.0))

    capital_recycling_rate = 0.0
    if n_fills > 0:
        capital_recycling_rate = (capital_returned_via_merges_usd + inv_returned_via_asks) / max(n_fills, 1)

    composite = 0.0
    if n_fills > 0:
        gross_factor = (1.0 - as_drag_per_fill_avg / gross_per_fill_avg) if gross_per_fill_avg > 0 else 1.0
        gross_factor = max(0.0, min(1.0, gross_factor + 0.1))  # bound
        composite = (
            sign_positive
            * max(0.0, min(1.0, capital_recycling_rate))
            * max(0.0, gross_factor)
            * (1.0 - min(1.0, tail_rate))
            * math.log(1 + n_fills)
        )
    return {
        "fill_count": n_fills,
        "pnl_worst_sum": pnl_worst_sum,
        "sign_positive": sign_positive,
        "capital_recycling_rate": capital_recycling_rate,
        "as_drag_per_fill_avg": as_drag_per_fill_avg,
        "gross_per_fill_avg": gross_per_fill_avg,
        "tail_rate": tail_rate,
        "merges_count": merges_count,
        "capital_returned_via_merges_usd": capital_returned_via_merges_usd,
        "inv_returned_via_asks_usd": inv_returned_via_asks,
        "composite_score": composite,
    }


__all__ = ["compute_composite_score"]
