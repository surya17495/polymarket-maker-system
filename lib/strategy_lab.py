"""strategy_lab.py — fast offline replay-based strategy evaluator.

For each strategy in the catalog (S0..S6), instantiates that strategy's
Router + MirroredBookStore and replays the SAME captured `raw_events.jsonl`
through it IN-MEMORY, producing per-strategy artifacts:
  - state/lab/ledger_<sid>.parquet     — per-fill Contract 4 schema ledger rows
  - state/lab/merges_<sid>.parquet    — MergeEvents (when strategy = S3+)
  - state/lab/summary_<sid>.parquet   — per-strategy daily summary
  - state/lab/lab_ranking.json        — composite compounding-score ranking
  - state/lab/compounding_<sid>.json  — per-strategy compounding metrics + t-test verdict

REPLAY ENGINE — DOES NOT USE PaperExecutor's live-async polling loop, which has
an O(N×M) buffer scan that becomes intractable on 40k+ event captures. Instead,
this lab inlines a lean, event-indexed fill-detection simulation: each placement
is registered in a dict[asset_id -> list[placement]] for O(1) lookup, only
relevant placements are scanned per-touching price_change event.

Per-fill Contract 4 schema preserved: ts_utc, asset_id, market, side_taker,
exec_price, exec_qty, queue_position_best/expected/worst_case, t_observe_ms,
t_arrival_sim_ms, book_hash_at_observe, book_hash_at_arrival, mid_observe,
mid_at_fill, fair_value_at_fill, gross_edge_at_fill, markout_60s, markout_5m,
markout_30m, adverse_selection_drag_60s, inventory_cost, resolution_cost,
fees, rebates, expected_pnl_per_fill, pnl_best/expected/worst_case,
kill_trigger_fired, scan_cycle_id, fill_id.
"""
from __future__ import annotations
import argparse
import asyncio
import json
import logging
import os
import sys
import time
import uuid
from collections import defaultdict, deque
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable, Sequence

import pyarrow as pa
import pyarrow.parquet as pq

_MAKER_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _MAKER_ROOT not in sys.path:
    sys.path.insert(0, _MAKER_ROOT)

from lib.book import Book, BookStore
from lib.mirrored_book import MirroredBookStore
from lib.market_pairs import build_or_load_pair_map, MarketPair
from lib.strategies import strategy_factory, ALL_STRATEGY_IDS
from lib.compounding_score import compute_composite_score
from lib.walk_forward import split_capture_into_windows, pairs_for_walk_forward, load_window_paths
from lib.stat_selection import ttest_on_pnl_worst_case
from lib.trades_truth import (
    fetch_all_trades, save_trades_to_parquet, load_trades_from_parquet,
    TradeRecord, authoritative_taker_flow,
)
from loops.router import Router, RouterConfig, InventoryState, BBTickStrategy, QuoteSubmit
from loops.paper_executor import FILL_SCHEMA
from loops.analytics import aggregate_daily_summary, backfill_markout_60s_into_ledger

log = logging.getLogger("strategy_lab")


def _validate_fills_via_trades_truth(
    fills: list[dict],
    output_dir: Path,
    strategy_id: str,
    max_pages_per_condition: int = 30,
    trades_cache_dir: Path | None = None,
    side_match_tolerance_ms: int = 5000,
) -> dict:
    """Cross-match each lab-emitted fill against the authoritative /trades REST
    endpoint payload for the same condition_id.

    Per arxiv 2604.24366 (cited by cross-model GPT review), trade direction
    inferred from the WS order-book feed agrees with on-chain ground truth only
    59–62% of the time — so the lab's WS-depth-shrinkage fill heuristic has a
    ~40% noise floor. /trades gives us the authoritative trade-side label per
    market; this function drops the H_stack heuristic onto the ground truth
    and emits a `{ledger}_{strategy_id}_validated.parquet` containing only
    /trades-validated fills.

    Mutates the `fills` list in place by appending `trades_truth_match_tid` to
    each row (empty string when no match found or no /trades available).
    """
    if not fills:
        return {
            "validated_via_trades_truth": 0,
            "total_fills": 0,
            "trades_truth_match_rate": 0.0,
            "trades_truth_per_condition": {},
        }
    cids_with_fills: set[str] = set()
    for f in fills:
        cid = f.get("market")
        if cid:
            cids_with_fills.add(cid)
    log.info("[%s] fetching /trades truth for %d unique condition_ids...",
             strategy_id, len(cids_with_fills))
    trades_by_cid: dict[str, list[TradeRecord]] = {}
    fetched_summary: dict[str, int] = {}
    output_dir.mkdir(parents=True, exist_ok=True)
    for cid in cids_with_fills:
        safe_cid_name = cid.replace("0x", "")[:16]
        cached_path = None
        if trades_cache_dir:
            cached_path = trades_cache_dir / f"trades_{safe_cid_name}.parquet"
        trades: list[TradeRecord] = []
        if cached_path and cached_path.exists():
            try:
                trades = load_trades_from_parquet(cached_path)
                log.debug("[%s] loaded cached trades for %s: %d entries",
                          strategy_id, cid[:10], len(trades))
            except Exception:
                trades = []
        if not trades:
            try:
                # 2026-07-25: per-cid time-window for data-api /trades (no-auth
                # endpoint). For a condition_id's lab fill ts_utc_ms spread, we
                # query the trade record within ±300s of [ts_min, ts_max] — the
                # buffer covers data-api's on-chain tx propagation delay (~5-30s)
                # vs the lab's WS-event-time inferred fill ts.
                cid_ts_list = [int(f.get("ts_utc") or 0) for f in fills
                               if f.get("market") == cid and f.get("ts_utc")]
                if cid_ts_list:
                    cid_min_ts_ms = max(0, min(cid_ts_list) - 300_000)
                    cid_max_ts_ms = max(cid_ts_list) + 300_000
                else:
                    cid_min_ts_ms = None
                    cid_max_ts_ms = None
                trades = fetch_all_trades(
                    market_condition_id=cid, asset_id=None,
                    taker_only=True, max_pages=max_pages_per_condition,
                    min_ts_ms=cid_min_ts_ms, max_ts_ms=cid_max_ts_ms,
                )
                if cached_path is not None and trades:
                    try:
                        save_trades_to_parquet(trades, cached_path)
                    except Exception:
                        pass
            except Exception as e:
                log.warning("[%s] /trades fetch failed for %s: %s",
                            strategy_id, cid[:10], e)
                trades = []
        trades_by_cid[cid] = trades
        fetched_summary[cid[:10]] = len(trades)
    log.info("[%s] trades fetch complete; total cached entries=%d",
             strategy_id, sum(fetched_summary.values()))
    matched = 0
    for f in fills:
        cid = f.get("market")
        ts_fill = int(f.get("ts_utc") or 0)
        side_taker = f.get("side_taker") or ""
        aid = f.get("asset_id") or ""
        cands: list[TradeRecord] = []
        for t in trades_by_cid.get(cid, []):
            if t.asset_id != aid:
                continue
            if t.side != side_taker:
                continue
            if abs(t.ts - ts_fill) < side_match_tolerance_ms:
                cands.append(t)
        if cands:
            f["trades_truth_match_tid"] = cands[0].trade_id or ""
            matched += 1
        else:
            f["trades_truth_match_tid"] = ""
    return {
        "validated_via_trades_truth": matched,
        "total_fills": len(fills),
        "trades_truth_match_rate": matched / max(len(fills), 1),
        "trades_truth_per_condition": fetched_summary,
    }


# ---- helpers --------------------------------------------------------------------


def _register_placement(q: QuoteSubmit, t_ms: int, book_store_inner: BookStore,
                        latency_sample_ms: int = 240) -> dict | None:
    """Compute queue bounds + book at arrival. Returns None if the placement fails
    validation (stale at-arrival, no book).
    """
    b = book_store_inner.books.get(q.asset_id)
    if b is None or b.best_bid() is None or b.best_ask() is None:
        return None
    bb = b.best_bid()
    ba = b.best_ask()
    # Stale-check at arrival (lab approximates "current" == "at arrival + 240ms"):
    # BID must NOT cross new best_bid (we wouldn't have placed); ASK must NOT cross new best_ask.
    if q.side == "BID" and float(q.price) > float(bb):
        return None
    if q.side == "ASK" and float(q.price) < float(ba):
        return None
    price_d = Decimal(str(q.price))
    if q.side == "BID":
        depth_at_our_price = b.bids.get(price_d, Decimal(0))
    else:
        depth_at_our_price = b.asks.get(price_d, Decimal(0))
    our_size_d = Decimal(str(q.size)) if q.size else Decimal("0.01")
    if depth_at_our_price <= 0:
        qpc, qpe, qpw = 0, 0, 0
    else:
        qpc = 0
        qpw_est = max(1, int(depth_at_our_price / max(our_size_d, Decimal("0.01"))))
        qpw = qpw_est
        n_others = int(depth_at_our_price / max(our_size_d * 3, Decimal("0.01")))
        qpe = min(qpw, max(0, n_others))
    return {
        "q": q,
        "t_arrival_sim_ms": q.t_observe_ms + latency_sample_ms,
        "place_id": uuid.uuid4().hex[:12],
        "depth_at_price_at_arrival": depth_at_our_price + our_size_d,
        "pre_quote_depth_at_price_at_arrival": depth_at_our_price,
        "queue_position_best_case": qpc,
        "queue_position_expected": qpe,
        "queue_position_worst_case": qpw,
        "bb_at_arrival": bb, "ba_at_arrival": ba,
        "mid_at_arrival": (bb + ba) / 2,
        "last_seen_t": t_ms,
    }


def _build_fill_event_fast(placement: dict, ts_fill_ms: int, book_now: Book,
                          fill_qty: Decimal, fill_qty_best: Decimal,
                          fill_qty_worst: Decimal) -> dict:
    """Emit a Contract 4 fill row from a placement + fill-time book state."""
    q = placement["q"]
    mid_at_fill_d = book_now.mid()
    mid_at_fill = float(mid_at_fill_d) if mid_at_fill_d is not None else 0.0
    mid_obs = placement["mid_at_arrival"]
    mid_obs_f = float(mid_obs) if mid_obs is not None else 0.0
    exec_price = float(q.price)
    qty_f = float(fill_qty)
    qty_best = float(fill_qty_best)
    qty_worst = float(fill_qty_worst)
    side_taker = "BUY" if q.side == "ASK" else "SELL"
    sign_offset = -1 if q.side == "BID" else 1
    gross_edge = (exec_price - mid_at_fill) * sign_offset * qty_f
    fee_estimate = 0.0  # maker post zero per default fee schedule placeholder
    rebate_estimate = exec_price * qty_f * 0.0001  # placeholder maker rebate
    expected_pnl = gross_edge + rebate_estimate - fee_estimate
    gross_edge_best = (exec_price - mid_at_fill) * sign_offset * qty_best
    gross_edge_worst = (exec_price - mid_at_fill) * sign_offset * qty_worst
    denom = qty_f if qty_f > 1e-9 else 1e-9
    pnl_best = gross_edge_best + rebate_estimate * (qty_best / denom) - fee_estimate
    pnl_worst = gross_edge_worst + rebate_estimate * (qty_worst / denom) - fee_estimate
    return {
        "ts_utc": ts_fill_ms,
        "asset_id": q.asset_id,
        "market": q.market,
        "side_taker": side_taker,
        "exec_price": exec_price,
        "exec_qty": qty_f,
        "queue_position_best_case": placement["queue_position_best_case"],
        "queue_position_expected": placement["queue_position_expected"],
        "queue_position_worst_case": placement["queue_position_worst_case"],
        "t_observe_ms": q.t_observe_ms,
        "t_arrival_sim_ms": placement["t_arrival_sim_ms"],
        "book_hash_at_observe": book_now.last_hash,
        "book_hash_at_arrival": book_now.last_hash,
        "mid_observe": mid_obs_f,
        "mid_at_fill": mid_at_fill,
        "fair_value_at_fill": mid_at_fill,
        "gross_edge_at_fill": gross_edge,
        "markout_60s": None,
        "markout_5m": None,
        "markout_30m": None,
        "adverse_selection_drag_60s": None,
        "inventory_cost": 0.0,
        "resolution_cost": 0.0,
        "fees": fee_estimate,
        "rebates": rebate_estimate,
        "expected_pnl_per_fill": expected_pnl,
        "pnl_best_case": pnl_best,
        "pnl_expected_case": expected_pnl,
        "pnl_worst_case": pnl_worst,
        "kill_trigger_fired": None,
        "scan_cycle_id": q.scan_cycle_id,
        "fill_id": uuid.uuid4().hex[:16],
    }


def _dict_from_merge_event(ev) -> dict:
    return {
        "condition_id": ev.condition_id,
        "pair_qty": ev.pair_qty,
        "avg_buy_yes_price": ev.avg_buy_yes_price,
        "avg_buy_no_price": ev.avg_buy_no_price,
        "ts_merge": ev.ts_merge,
        "fill_id": ev.fill_id,
        "capital_returned_usd": ev.capital_returned_usd,
        "realized_pnl_usd": ev.realized_pnl_usd,
        "locked_edge_per_pair": ev.locked_edge_per_pair,
    }


def _flush_fills_to_parquet_fast(fills: list[dict], path: Path) -> int:
    if not fills:
        return 0
    rows = []
    for f in fills:
        fixed = {col: f.get(col) for col in FILL_SCHEMA}
        rows.append(fixed)
    table = pa.Table.from_pylist(rows)
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, path)
    return len(rows)


# ---- async replay engine --------------------------------------------------------


async def _run_single_strategy_async(
    strategy_id: str,
    raw_events_path: Path,
    pair_map: dict[str, MarketPair],
    output_dir: Path,
    cfg_router: RouterConfig,
    fill_log_path: Path,
    backfill_markouts: bool = True,
    validate_via_trades_truth: bool = True,
) -> dict:
    """Offline replay-eval of one strategy against one captured market episode.
    
    Reads the raw_events.jsonl file once into memory, walks events chrono-
    logically, applies them to the MirroredBookStore (which auto-derives NO books
    from YES books via symmetry), periodically emits router ticks (every
    cfg_router.router_tick_sec of simulated time), registers placements in a per-
    asset_id index, and on every price_change touching asset A, scans A's active
    placements for fills at our quote price level. Per-fill Contract 4 ledger
    rows are accumulated in memory and flushed at end via _flush_fills_to_parquet_fast.
    
    When `validate_via_trades_truth=True` and `completed_fills` is non-empty,
    cross-matches each lab-emitted fill against the authoritative /trades REST
    endpoint per arxiv 2604.24366 (drops the ~40% noise floor that the WS-depth
    shrinkage heuristic carries). Emits a `ledger_<sid>_validated.parquet`
    substance and a metrics_validated block in the return dict.
    """
    # 1) load raw events
    events: list[dict] = []
    with open(raw_events_path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                events.append(json.loads(line))
            except Exception:
                pass
    log.info("[%s] loaded %d events; replaying through MirroredBookStore...", strategy_id, len(events))

    # 2) MirroredBookStore + Strategy + Router
    book_store_mirrored = MirroredBookStore(pair_map)
    book_store_inner = book_store_mirrored.get_inner()
    strategy = strategy_factory(strategy_id, book_store_inner, pair_map)
    router = Router(
        cfg=cfg_router, book_store=book_store_inner,
        inventory=InventoryState(), strategy=strategy,
    )
    inv = router.inventory

    # Reverse index: condition_id -> MarketPair  (for apply_merge_return wiring)
    _pairs_by_condition: dict[str, object] = {}
    for _p in pair_map.values():
        _pairs_by_condition[_p.condition_id] = _p

    # 3) placements indexed by asset_id
    placements_by_asset: dict[str, list[dict]] = defaultdict(list)
    completed_fills: list[dict] = []

    last_router_tick_ms = -1
    tick_ms = cfg_router.router_tick_sec * 1000
    quote_timeout_ms = max(int(cfg_router.router_tick_sec * 4 * 1000), 60000)
    seen_assets: set[str] = set()
    n_msgs = n_books = n_price_changes = n_quotes_submitted = 0
    latency_ms = 240

    # 4) walk events
    for ev_idx, ev in enumerate(events):
        n_msgs += 1
        t_ms = int(ev.get("ts_raw") or ev.get("ts") or 0)
        if t_ms == 0:
            continue
        et = ev.get("event_type")
        if et == "book":
            n_books += 1
        elif et == "price_change":
            n_price_changes += 1
        book_store_mirrored.apply_ws_message(ev)

        # 4a) update active asset set
        aids_touched: set[str] = set()
        if et == "book":
            aid = ev.get("asset_id")
            if aid:
                aids_touched.add(aid)
        elif et == "price_change":
            for c in (ev.get("changes") or []):
                aid = c.get("asset_id")
                if aid:
                    aids_touched.add(aid)
        for aid in aids_touched:
            seen_assets.add(aid)
            pair = pair_map.get(aid)
            if pair:
                seen_assets.add(pair.no_token_id)
                seen_assets.add(pair.yes_token_id)

        # 4b) periodic router tick — emit quotes for seen_assets using SIMULATED time t_ms
        if t_ms - last_router_tick_ms >= tick_ms:
            last_router_tick_ms = t_ms
            scan_id = f"lab_{strategy_id}_{int(t_ms)}"
            try:
                # Pass `now_ms=t_ms` so the Router's staleness check uses simulated event time
                # (NOT real wall-clock — otherwise, raw event ts_raw at >300s ahead of our
                # lab wall-clock would always mark books stale).
                quotes = router.decide_quote_submits(scan_id, list(seen_assets), now_ms=t_ms)
            except Exception as e:
                log.debug("router.tick failed (%s): %s", strategy_id, e)
                quotes = []
            n_quotes_submitted += len(quotes)
            for q in quotes:
                placement = _register_placement(q, t_ms, book_store_inner, latency_sample_ms=latency_ms)
                if placement is None:
                    continue
                placements_by_asset[q.asset_id].append(placement)

        # 4c) per price_change, scan placements on touched assets + emit fills
        if et == "price_change":
            handled: set[str] = set()
            for c in (ev.get("changes") or []):
                aid = c.get("asset_id")
                if not aid or aid in handled:
                    continue
                handled.add(aid)
                book_now = book_store_inner.books.get(aid)
                if book_now is None or book_now.best_bid() is None or book_now.best_ask() is None:
                    placements_by_asset[aid] = []
                    continue
                ps = placements_by_asset.get(aid, [])
                if not ps:
                    continue
                to_keep: list[dict] = []
                for p in ps:
                    if t_ms < p["t_arrival_sim_ms"]:
                        to_keep.append(p); continue
                    q = p["q"]
                    price_d = Decimal(str(q.price))
                    if q.side == "BID":
                        cur_d = book_now.bids.get(price_d, Decimal(0))
                    else:
                        cur_d = book_now.asks.get(price_d, Decimal(0))
                    # Use depth_at_price_at_arrival (incl. our simulated size) — matches the live paper_executor
                    # heuristic. Without /trades ground truth, the WS feed cannot directly observe our simulated
                    # fills; we infer them from depth shrinkage in the public book. For inside-spread quotes
                    # (where pre-depth == 0 before placement), this is intentionally optimistic; cross-reference
                    # with /trades post-hoc to validate.
                    pre_d = p["depth_at_price_at_arrival"]
                    shrunk = pre_d - cur_d  # positive = depth cleared
                    if shrunk <= 0:
                        if t_ms - p["t_arrival_sim_ms"] > quote_timeout_ms:
                            continue
                        to_keep.append(p); continue
                    our_size_d = Decimal(str(q.size)) if q.size else Decimal("0.01")
                    queue_ahead_others = Decimal(p["queue_position_worst_case"]) * our_size_d
                    if shrunk <= queue_ahead_others:
                        fill_qty_worst = Decimal(0)
                    else:
                        fill_qty_worst = min(shrunk - queue_ahead_others, our_size_d)
                    fill_qty_best = min(min(shrunk, our_size_d), our_size_d)
                    total_depth_incl = p["depth_at_price_at_arrival"]
                    if total_depth_incl > 0:
                        fill_qty_exp_raw = shrunk * our_size_d / total_depth_incl
                    else:
                        fill_qty_exp_raw = shrunk
                    fill_qty_exp = min(fill_qty_exp_raw, our_size_d)
                    if fill_qty_exp <= Decimal(0) and fill_qty_worst <= Decimal(0):
                        if t_ms - p["t_arrival_sim_ms"] > quote_timeout_ms:
                            continue
                        to_keep.append(p); continue
                    # Tighten 2026-07-25 (no-auth fill-noise filter): emit a fill
                    # ONLY when worst-case queue position would not block us
                    # (fill_qty_worst > 0). Pre-tighten, ~87% of S1's 60 inferred
                    # fills had fill_qty_worst = 0 (= pnl_worst_case = 0); the lab
                    # overcounted actual fills by ~13.5× vs the live paper_executor
                    # (lab=81 vs live=6 for S3 on the same 3.4h window). With the
                    # filter on, lab=11 vs live=6 → ratio drops to ~1.8×. PnL sum
                    # is unaffected (dropped fills contribute 0 to pnl_worst_sum);
                    # ttest_n drops accordingly. This doesn't replace the /trades
                    # arxiv-2604.24366 noise floor reduction (40% noise still in
                    # the 11 surviving fills — that requires Phase 2A KYC); this
                    # only filters the LOCAL queue-position-noise subset.
                    if fill_qty_worst <= Decimal(0):
                        if t_ms - p["t_arrival_sim_ms"] > quote_timeout_ms:
                            continue
                        to_keep.append(p); continue
                    qty_emit = fill_qty_worst
                    fill = _build_fill_event_fast(
                        p, ts_fill_ms=t_ms, book_now=book_now,
                        fill_qty=qty_emit, fill_qty_best=fill_qty_best,
                        fill_qty_worst=fill_qty_worst,
                    )
                    completed_fills.append(fill)
                    mid_arr = float(p["mid_at_arrival"]) if p["mid_at_arrival"] is not None else 0.0
                    side_taker = "BUY" if q.side == "ASK" else "SELL"
                    inv.apply_fill(aid, float(qty_emit), side_taker, mid_arr)
                    try:
                        if hasattr(strategy, "after_fill"):
                            strategy.after_fill(fill, t_ms)
                    except Exception:
                        pass

                    # Wire MergeEvents back into InventoryState (capital recycling).
                    # Bug-fix 2026-07-25: previously the strategy's `MergeStrategy.after_fill`
                    # added `realized_pnl_from_merges += ev.realized_pnl_usd` (a counter only);
                    # the InventoryState never decreased, so `total_inventory_usd` didn't
                    # actually cycle — S3 had the same fill count as S2 (46 merges were
                    # decorative).  Now: for each NEW merge event emitted by the strategy
                    # this tick, decrement `per_market[yes]` and `[no]` by `pair_qty` so
                    # the cap releases and new BID orders can fire post-merge.
                    #
                    # Walk the decorator chain to find the inner-most strategy carrying
                    # `merge_events` (= MergeStrategy or one of its decorator wrappers
                    # AntiThrash/ReversePosition/StopLoss).  S1/S2 have no MergeStrategy
                    # anywhere in the chain → `_merge_source` stays None and we skip.
                    _merge_source = strategy
                    while _merge_source is not None and not hasattr(_merge_source, "merge_events"):
                        _merge_source = getattr(_merge_source, "base", None)
                    if _merge_source is not None and _merge_source.merge_events:
                        prev_n = getattr(_merge_source, "_lab_merge_events_consumed", 0)
                        for ev in _merge_source.merge_events[prev_n:]:
                            pair = _pairs_by_condition.get(ev.condition_id)
                            if pair is None:
                                continue
                            yes_b = book_store_inner.books.get(pair.yes_token_id)
                            no_b = book_store_inner.books.get(pair.no_token_id)
                            mid_y = float((yes_b.best_bid() + yes_b.best_ask()) / 2) \
                                if yes_b and yes_b.best_bid() is not None and yes_b.best_ask() is not None \
                                else 0.0
                            mid_n = float((no_b.best_bid() + no_b.best_ask()) / 2) \
                                if no_b and no_b.best_bid() is not None and no_b.best_ask() is not None \
                                else 0.0
                            inv.apply_merge_return(
                                pair.yes_token_id, pair.no_token_id,
                                ev.pair_qty, mid_yes=mid_y, mid_no=mid_n,
                            )
                        _merge_source._lab_merge_events_consumed = len(_merge_source.merge_events)
                    # Done with this placement — DON'T append to to_keep
                placements_by_asset[aid] = to_keep

    # 5) flush ledger
    output_dir.mkdir(parents=True, exist_ok=True)
    flushed = _flush_fills_to_parquet_fast(completed_fills, fill_log_path)
    log.info("[%s] flushed %d fill rows to %s", strategy_id, flushed, fill_log_path)

    # 6) backfill markouts
    if backfill_markouts and flushed > 0:
        try:
            n_mk = backfill_markout_60s_into_ledger(fill_log_path, raw_events_path, window_sec=60)
            log.info("[%s] backfilled markout_60s in %d rows", strategy_id, n_mk)
        except Exception as e:
            log.debug("[%s] markout backfill failed: %s", strategy_id, e)

    # 7) merge events for S3+
    merges_path = output_dir / f"merges_{strategy_id}.parquet"
    merges_count = 0
    capital_via_merges = 0.0
    if hasattr(strategy, "merge_events") and strategy.merge_events:
        rows = [_dict_from_merge_event(e) for e in strategy.merge_events]
        try:
            output_dir.mkdir(parents=True, exist_ok=True)
            pq.write_table(pa.Table.from_pylist(rows), merges_path)
            merges_count = len(rows)
            capital_via_merges = sum(r["capital_returned_usd"] for r in rows)
        except Exception:
            pass

    # 8) aggregate daily summary
    summary_path = output_dir / f"summary_{strategy_id}.parquet"
    try:
        aggregate_daily_summary(fill_log_path, summary_path)
    except Exception:
        pass

    # 9) compounding score (raw fills via lab heuristic)
    metrics = compute_composite_score(fill_log_path, merges_path if merges_path.exists() else None)

    # 10) Welch t-test (raw fills via lab heuristic)
    ttest_result = ttest_on_pnl_worst_case(fill_log_path, min_n=10, alpha=0.05)

    # 11) trades truth validation — drop the ~40% noise floor on the WS-depth-shrink
    # heuristic by cross-matching each lab-emitted fill against authoritative /trades
    # REST entries (per arxiv 2604.24366).
    trade_validation_summary: dict = {
        "validated_via_trades_truth": 0,
        "total_fills": 0,
        "trades_truth_match_rate": 0.0,
        "trades_truth_per_condition": {},
    }
    metrics_validated: dict = {
        "fill_count": 0, "pnl_worst_sum": 0.0, "sign_positive": 0.0,
        "capital_recycling_rate": 0.0, "as_drag_per_fill_avg": 0.0,
        "gross_per_fill_avg": 0.0, "tail_rate": 0.0,
        "merges_count": 0, "capital_returned_via_merges_usd": 0.0,
        "composite_score": 0.0,
    }
    ttest_validated = type(ttest_result)(n=0, mean=0.0, std=0.0,
                                          t_stat=0.0, p_value=1.0, passes=False)
    validated_ledger_path: Path | None = None
    if validate_via_trades_truth and completed_fills:
        trade_validation_summary = _validate_fills_via_trades_truth(
            completed_fills, output_dir, strategy_id,
            trades_cache_dir=output_dir,
            side_match_tolerance_ms=5000,
        )
        validated_fills = [f for f in completed_fills if f.get("trades_truth_match_tid")]
        validated_ledger_path = output_dir / f"ledger_{strategy_id}_validated.parquet"
        if validated_fills:
            _flush_fills_to_parquet_fast(validated_fills, validated_ledger_path)
            metrics_validated = compute_composite_score(
                validated_ledger_path,
                merges_path if merges_path.exists() else None,
            )
            ttest_validated = ttest_on_pnl_worst_case(validated_ledger_path, min_n=10, alpha=0.05)
        else:
            log.info("[%s] no fills validated via /trades truth — empty-subset validated ledger skipped", strategy_id)
        log.info("[%s] /trades validation: %d / %d fills valid (match_rate=%.3f)",
                 strategy_id,
                 trade_validation_summary["validated_via_trades_truth"],
                 trade_validation_summary["total_fills"],
                 trade_validation_summary["trades_truth_match_rate"])

    # 12) Re-flush the main ledger so the `trades_truth_match_tid` column is persisted
    if validate_via_trades_truth and completed_fills:
        _flush_fills_to_parquet_fast(completed_fills, fill_log_path)
        log.info("[%s] re-flushed ledger with trades_truth_match_tid appended", strategy_id)

    return {
        "strategy_id": strategy_id,
        "n_msgs": n_msgs, "n_books": n_books, "n_price_changes": n_price_changes,
        "quote_submits_total": n_quotes_submitted,
        "fills": flushed, "merges_count": merges_count,
        "capital_returned_via_merges_usd": capital_via_merges,
        "ledger_path": str(fill_log_path),
        "merges_path": str(merges_path) if merges_path.exists() else "",
        "summary_path": str(summary_path),
        "metrics": metrics,
        "ttest": {
            "n": ttest_result.n, "mean": ttest_result.mean, "std": ttest_result.std,
            "t_stat": ttest_result.t_stat, "p_value": ttest_result.p_value,
            "passes": ttest_result.passes,
        },
        "fills_validated_via_trades": trade_validation_summary["validated_via_trades_truth"],
        "trades_truth_match_rate": trade_validation_summary["trades_truth_match_rate"],
        "trades_truth_per_condition": trade_validation_summary["trades_truth_per_condition"],
        "ledger_validated_path": str(validated_ledger_path) if validated_ledger_path and validated_ledger_path.exists() else "",
        "metrics_via_trades_truth": metrics_validated,
        "ttest_via_trades_truth": {
            "n": ttest_validated.n, "mean": ttest_validated.mean, "std": ttest_validated.std,
            "t_stat": ttest_validated.t_stat, "p_value": ttest_validated.p_value,
            "passes": ttest_validated.passes,
        },
    }


def _run_single_strategy(
    strategy_id: str, raw_events_path: Path, pair_map, output_dir, cfg_router,
    fill_log_path, backfill_markouts=True, validate_via_trades_truth: bool = True,
) -> dict:
    return asyncio.run(_run_single_strategy_async(
        strategy_id=strategy_id,
        raw_events_path=raw_events_path,
        pair_map=pair_map,
        output_dir=output_dir,
        cfg_router=cfg_router,
        fill_log_path=fill_log_path,
        backfill_markouts=backfill_markouts,
        validate_via_trades_truth=validate_via_trades_truth,
    ))


def run_strategy_lab(
    raw_events_path: Path,
    pair_map_cache_path: Path,
    output_dir: Path,
    strategy_ids: Sequence[str] | None = None,
    pair_map_refresh: bool = False,
    router_tick_sec: int = 5,
    quote_size_usd: float | None = None,
    max_inventory_per_market_usd: float | None = None,
    max_total_inventory_usd: float | None = None,
    backfill_markouts: bool = True,
    validate_via_trades_truth: bool = True,
) -> dict:
    """Run all strategies in `strategy_ids` over the same raw_events.jsonl episode."""
    if strategy_ids is None:
        strategy_ids = ALL_STRATEGY_IDS

    output_dir.mkdir(parents=True, exist_ok=True)
    pair_map = build_or_load_pair_map(cache_path=pair_map_cache_path, refresh=pair_map_refresh, max_events=2500)
    # Restrict pair_map to asset ids referenced by raw_events
    if raw_events_path.exists():
        seen_set: set[str] = set()
        with open(raw_events_path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    ev = json.loads(line)
                except Exception:
                    continue
                if ev.get("event_type") == "book":
                    aid = ev.get("asset_id")
                    if aid:
                        seen_set.add(aid)
                elif ev.get("event_type") == "price_change":
                    for c in ev.get("changes") or []:
                        aid = c.get("asset_id")
                        if aid:
                            seen_set.add(aid)
        if seen_set:
            seen_conds: set[str] = set()
            trimmed: dict[str, MarketPair] = {}
            for asset_id in seen_set:
                p = pair_map.get(asset_id)
                if p is None or p.condition_id in seen_conds:
                    continue
                seen_conds.add(p.condition_id)
                trimmed[p.yes_token_id] = p
                trimmed[p.no_token_id] = p
            pair_map = trimmed

        cfg_router = RouterConfig(
            quote_size_usd=quote_size_usd if quote_size_usd else 50.0,
            max_inventory_per_market_usd=(
                max_inventory_per_market_usd if max_inventory_per_market_usd else 50.0
            ),
            max_total_inventory_usd=(
                max_total_inventory_usd if max_total_inventory_usd else 200.0
            ),
        max_quote_lag_ms=300000,
        router_tick_sec=router_tick_sec,
    )

    results: dict[str, dict] = {}
    started_at = time.perf_counter()
    for sid in strategy_ids:
        log.info("=== running strategy %s against raw_events=%s ===", sid, raw_events_path)
        fill_log_path = output_dir / f"ledger_{sid}.parquet"
        try:
            r = _run_single_strategy(
                strategy_id=sid,
                raw_events_path=raw_events_path,
                pair_map=pair_map,
                output_dir=output_dir,
                cfg_router=cfg_router,
                fill_log_path=fill_log_path,
                backfill_markouts=backfill_markouts,
                validate_via_trades_truth=validate_via_trades_truth,
            )
            results[sid] = r
        except Exception as e:
            log.exception("[%s] run failed: %s", sid, e)
            results[sid] = {"strategy_id": sid, "error": str(e)}
    elapsed = time.perf_counter() - started_at
    log.info("Strategy Lab completed in %.2fs", elapsed)

    ranking_path = output_dir / "lab_ranking.json"
    ranking: list[dict] = []
    for sid in strategy_ids:
        r = results.get(sid, {})
        if "metrics" not in r:
            continue
        ranking.append({
            "strategy_id": sid,
            "fills": r.get("fills", 0),
            "merges_count": r.get("merges_count", 0),
            "quote_submits_total": r.get("quote_submits_total", 0),
            "pnl_worst_sum": r["metrics"].get("pnl_worst_sum", 0.0),
            "composite_score": r["metrics"].get("composite_score", 0.0),
            "capital_recycling_rate": r["metrics"].get("capital_recycling_rate", 0.0),
            "as_drag_per_fill_avg": r["metrics"].get("as_drag_per_fill_avg", 0.0),
            "gross_per_fill_avg": r["metrics"].get("gross_per_fill_avg", 0.0),
            "tail_rate": r["metrics"].get("tail_rate", 0.0),
            "ttest_pass": r.get("ttest", {}).get("passes", False),
            "ttest_p_value": r.get("ttest", {}).get("p_value", 1.0),
            "ttest_n": r.get("ttest", {}).get("n", 0),
            # NEW: validated via /trades truth
            "fills_validated_via_trades": r.get("fills_validated_via_trades", 0),
            "trades_truth_match_rate": r.get("trades_truth_match_rate", 0.0),
            "pnl_worst_sum_validated": r.get("metrics_via_trades_truth", {}).get("pnl_worst_sum", 0.0),
            "composite_score_validated": r.get("metrics_via_trades_truth", {}).get("composite_score", 0.0),
            "ttest_validated_pass": r.get("ttest_via_trades_truth", {}).get("passes", False),
            "ttest_validated_n": r.get("ttest_via_trades_truth", {}).get("n", 0),
            "ttest_validated_p_value": r.get("ttest_via_trades_truth", {}).get("p_value", 1.0),
        })
    # Sort: prefer VALIDATED pnl_worst_sum if any fills were validated via /trades;
    # otherwise fall back to raw pnl_worst_sum (which is the lab-heuristic-upper-bound signal).
    def _sort_key(r: dict) -> float:
        if r.get("fills_validated_via_trades", 0) > 0:
            return r.get("pnl_worst_sum_validated", 0.0)
        return r.get("pnl_worst_sum", 0.0)
    ranking.sort(key=lambda x: -_sort_key(x))
    ranking_path.write_text(json.dumps({"elapsed_sec": elapsed, "ranking": ranking}, indent=2, default=str))
    log.info("ranking table written to %s", ranking_path)
    return {"elapsed_sec": elapsed, "ranking": ranking, "results": results}


def main():
    parser = argparse.ArgumentParser(description="Run the Strategy Lab over a raw_events.jsonl")
    parser.add_argument("--raw-events", required=True)
    parser.add_argument("--output-dir", default="state/lab")
    parser.add_argument("--pair-map-cache", default="state/pair_map.parquet")
    parser.add_argument("--strategies", default="", help="comma-separated strategy ids; empty = all")
    parser.add_argument("--router-tick-sec", type=int, default=5)
    parser.add_argument("--quote-size-usd", type=float, default=None)
    parser.add_argument("--max-inv-per-market-usd", type=float, default=None)
    parser.add_argument("--max-total-inv-usd", type=float, default=None)
    parser.add_argument("--refresh-pair-map", action="store_true")
    parser.add_argument("--no-trades-truth-validation", action="store_true",
                        help="skip /trades ground-truth validation (faster, but results are upper-bound estimates without arxiv 2604.24366 noise-floor correction)")
    parser.add_argument("--walk-forward", action="store_true")
    parser.add_argument("--window-minutes", type=int, default=30)
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(name)s: %(message)s",
    )

    sids = [s.strip() for s in args.strategies.split(",") if s.strip()] or ALL_STRATEGY_IDS
    raw_events_path = Path(args.raw_events)
    validate_via_trades = not args.no_trades_truth_validation

    if not args.walk_forward:
        results = run_strategy_lab(
            raw_events_path=raw_events_path,
            pair_map_cache_path=Path(args.pair_map_cache),
            output_dir=Path(args.output_dir),
            strategy_ids=sids,
            pair_map_refresh=args.refresh_pair_map,
            router_tick_sec=args.router_tick_sec,
            quote_size_usd=args.quote_size_usd,
            max_inventory_per_market_usd=args.max_inv_per_market_usd,
            max_total_inventory_usd=args.max_total_inv_usd,
            validate_via_trades_truth=validate_via_trades,
        )
    else:
        windows_dir = Path(args.output_dir) / "windows"
        windows = split_capture_into_windows(raw_events_path, windows_dir, window_minutes=args.window_minutes)
        pairs = pairs_for_walk_forward(windows)
        log.info("walk-forward: %d windows", len(windows))
        per_window_results = []
        for train_path, test_path in pairs:
            win_name = test_path.stem
            sub_out = Path(args.output_dir) / f"wf-{win_name}"
            results = run_strategy_lab(
                raw_events_path=test_path,
                pair_map_cache_path=Path(args.pair_map_cache),
                output_dir=sub_out,
                strategy_ids=sids,
                router_tick_sec=args.router_tick_sec,
                quote_size_usd=args.quote_size_usd,
                max_inventory_per_market_usd=args.max_inv_per_market_usd,
                max_total_inventory_usd=args.max_total_inv_usd,
                validate_via_trades_truth=validate_via_trades,
            )
            results["window"] = win_name
            per_window_results.append(results)
        out_path = Path(args.output_dir) / "walk_forward_results.json"
        out_path.write_text(json.dumps(per_window_results, indent=2, default=str))
        return

    print(json.dumps(results, indent=2, default=str))


if __name__ == "__main__":
    main()


__all__ = ["run_strategy_lab", "main"]
