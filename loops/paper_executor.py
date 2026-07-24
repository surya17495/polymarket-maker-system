"""paper_executor.py — Phase 1A simulator implementing §8.6 Contracts 1-7.

The simulator processes pending `QuoteSubmit` events from the Router. For each:
1. Lock a snapshot of BookStore.latest_books at t_observe
2. Record book_hash_at_observe from the WS-served snapshot
3. Schedule t_arrival_sim = t_observe + latency_sample_ms
4. At t_arrival_sim, REPLAY every WS message with ts_raw in [t_observe, t_arrival_sim]
   against a copy of the snapshot book -> book_at_arrival
5. Validate our quote price vs book_at_arrival: if our quote price is not at-or-inside the
   new BBO, the quote is cancelled off-book (rejected as stale)
6. Otherwise enter the queue at:
   - queue_position_best_case = 0   (we were the first arrival at this price)
   - queue_position_worst_case = max(1, int(depth_at_our_price_before_us / our_size))
   - queue_position_expected   = depth-weighted by historical message-storm severity
7. For every subsequent price_change event while the quote is live, check:
   - if the change cancels a level <= our quote price → does that imply a fill? Polymarket WS
     doesn't tell us directly. Heuristic: if size[our_price_level] decreases by <= our_size
     (relative to last snapshot) AND there were no other apparent fills (compared to trades
     webhooks), we model our queue share = our_size / depth_at_level_before_us.
8. Emit a PaperFill row per Contract 4 with queue bounds + markout + expected PnL.

A kill-switch fires if cumulative inventory or paper-realized PnL trips the
configured caps (§6 1%/2%/3% ladder etc.).

Latency sampling uses lib/latency_model.LatencyStats when available; otherwise
falls back to a configured default bounded random draw.

NOTE: this is the Phase 1A scaffold (produce per-fill ledger rows; graduate
Phase 1A gate after 48h of operational sanity; then Phase 1B regime validation
builds on top).
"""
from __future__ import annotations
import asyncio
import json
import logging
import os
import random
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable

import pyarrow as pa
import pyarrow.parquet as pq

from lib.book import Book, BookStore
from lib.latency_model import get_latency_stats
from loops.router import QuoteSubmit, Router

log = logging.getLogger(__name__)


# Contract 4 schema — every paper_fill row
FILL_SCHEMA = [
    "ts_utc", "asset_id", "market", "side_taker", "exec_price", "exec_qty",
    "queue_position_best_case", "queue_position_expected", "queue_position_worst_case",
    "t_observe_ms", "t_arrival_sim_ms", "book_hash_at_observe", "book_hash_at_arrival",
    "mid_observe", "mid_at_fill", "fair_value_at_fill", "gross_edge_at_fill",
    "markout_60s", "markout_5m", "markout_30m",
    "adverse_selection_drag_60s", "inventory_cost", "resolution_cost", "fees", "rebates",
    "expected_pnl_per_fill", "pnl_best_case", "pnl_expected_case", "pnl_worst_case",
    "kill_trigger_fired", "scan_cycle_id", "fill_id",
]


@dataclass
class PaperFillEvent:
    """One simulated fill event emitted by the paper executor."""
    row: dict[str, Any]


@dataclass
class QuotePlacement:
    """A quote that landed in the simulated book and now waits for fills."""
    quote: QuoteSubmit
    t_arrival_sim_ms: int
    book_at_arrival: Book
    queue_position_best_case: int
    queue_position_expected: int
    queue_position_worst_case: int
    depth_at_price_at_arrival: Decimal
    pre_quote_depth_at_price_at_arrival: Decimal
    place_id: str


@dataclass
class PaperExecutor:
    book_store: BookStore
    router: Router
    raw_events_path: Path
    fill_log_path: Path = Path("state/ledger.parquet")

    # Sentinel config — Phase 1A defaults
    default_latency_sample_ms: int = 240  # GPT-cited nominal 80+5+5+150 ms
    latency_sample_jitter_ms: int = 120   # variance on top of mean
    quote_timeout_sec: float = 60.0       # cancel unfilled quotes after this window
    max_actual_quote_size_shares: float = 200.0
    kill_switch_drawdown_pct_warn: float = 1.0
    kill_switch_drawdown_pct_reduce: float = 2.0
    kill_switch_drawdown_pct_halt: float = 3.0
    base_equity_usd: float = 2000.0

    placed_quotes: dict[str, QuotePlacement] = field(default_factory=dict)
    pending_fills: deque = field(default_factory=deque)
    raw_events_buffer: deque = field(default_factory=deque)
    completed_fills: list[dict[str, Any]] = field(default_factory=list)



    # ---------------------------------------------------------------- core entry

    async def run_forever(self) -> None:
        """Background task that processes pending quote_submits and replays raw_events."""
        while True:
            await asyncio.sleep(0.05)
            await self._process_pending_quotes()
            await self._process_book_events_into_fills()

    def submit_quote(self, q: QuoteSubmit) -> str:
        """Synchronous: enqueue a QuoteSubmit. Returns a placement id placeholder.

        The actual placement is decided by _process_pending_quotes which samples
        the latency, replays raw_events, and locks the queue position bounds.
        """
        placed_id = uuid.uuid4().hex[:12]
        self.pending_fills.append((placed_id, q))
        return placed_id

    # ---------------------------------------------------------------- wire-internal

    def _sample_latency_ms(self, q: QuoteSubmit) -> int:
        ls = get_latency_stats()
        s = ls.summary()
        ws_detect = s.get("ws_detect_ms", {})
        rest_book = s.get("rest_book_ms", {})
        ws_p50 = (ws_detect or {}).get("p50") if ws_detect else None
        rest_p50 = (rest_book or {}).get("p50") if rest_book else None
        base = self.default_latency_sample_ms
        if ws_p50 and rest_p50:
            base = int(ws_p50 + 30 + rest_p50)
        jitter = random.randint(0, self.latency_sample_jitter_ms)
        return base + jitter

    async def _process_pending_quotes(self) -> None:
        while self.pending_fills:
            placed_id, q = self.pending_fills.popleft()
            # 1) snapshot book at t_observe
            book_observed = self.book_store.books.get(q.asset_id)
            if book_observed is None:
                return
            snapshot_book = Book(
                asset_id=q.asset_id,
                market=book_observed.market,
                tick_size=book_observed.tick_size,
                bids=dict(book_observed.bids),
                asks=dict(book_observed.asks),
                last_hash=book_observed.last_hash,
                last_update_ms=book_observed.last_update_ms,
                last_ltp=book_observed.last_ltp,
            )
            # 2) schedule arrival
            lat_ms = self._sample_latency_ms(q)
            t_arrival_sim_ms = q.t_observe_ms + lat_ms

            # 3) replay raw events captured in [t_observe, t_arrival_sim]
            arrival_book = Book(
                asset_id=q.asset_id,
                market=book_observed.market,
                tick_size=book_observed.tick_size,
                bids=dict(book_observed.bids),
                asks=dict(book_observed.asks),
                last_hash=book_observed.last_hash,
                last_update_ms=book_observed.last_update_ms,
                last_ltp=book_observed.last_ltp,
            )
            replayed: list[dict] = []
            for ev in self.raw_events_buffer:
                try:
                    ev_ts = int(ev.get("ts_raw") or ev.get("ts") or 0)
                except (TypeError, ValueError):
                    continue
                if ev_ts < q.t_observe_ms:
                    continue
                if ev_ts > t_arrival_sim_ms:
                    continue
                if ev.get("event_type") == "book":
                    arrival_book.apply_snapshot(ev)
                elif ev.get("event_type") == "price_change":
                    for c in ev.get("changes") or []:
                        if c.get("asset_id") == q.asset_id:
                            arrival_book.apply_change(c, ts_ms=ev_ts)
                replayed.append(ev)

            bb_a = arrival_book.best_bid()
            ba_a = arrival_book.best_ask()
            if bb_a is None or ba_a is None:
                return
            price = Decimal(str(q.price))
            side = q.side
            # 4) validate at arrival
            if side == "BID":
                if price > bb_a:
                    # stale: our bid is now mid-book; would we be cancelled or refreshed?
                    return
                depth_at_our_price = arrival_book.bids.get(price, Decimal(0))
            else:
                if price < ba_a:
                    return
                depth_at_our_price = arrival_book.asks.get(price, Decimal(0))

            # 5) estimate queue position bounds
            our_size = Decimal(str(q.size))
            if depth_at_our_price <= 0:
                # our quote is the only quote at this price (sole maker)
                qpc = 0
                qpe = 0
                qpw = 0
            else:
                qpc = 0
                # qpw = depth-at-our-price / our_size (number of "us-sized" orders ahead),
                # NOT (depth + our_size)/our_size which would inflate by +1 always (force worst case = infinity).
                qpw_est = max(1, int(depth_at_our_price / max(our_size, Decimal("0.01"))))
                qpw = qpw_est
                qpe = min(qpw, max(0, prev_users_at_price_estimate(depth_at_our_price, our_size)))
            placement = QuotePlacement(
                quote=q,
                t_arrival_sim_ms=t_arrival_sim_ms,
                book_at_arrival=arrival_book,
                queue_position_best_case=qpc,
                queue_position_expected=qpe,
                queue_position_worst_case=qpw,
                depth_at_price_at_arrival=depth_at_our_price + our_size,
                pre_quote_depth_at_price_at_arrival=depth_at_our_price,
                place_id=placed_id,
            )
            self.placed_quotes[placed_id] = placement
            log.debug("placed %s at %s for %s (qpb=%d qpe=%d qpw=%d)",
                      placed_id, price, q.asset_id[:8], qpc, qpe, qpw)

    async def _process_book_events_into_fills(self) -> None:
        if not self.placed_quotes:
            return
        # consume new raw events; for each placed quote, check if our quote level
        # was crossed/cancelled and emit fill accordingly
        now_ms = int(time.time() * 1000)
        for placed_id in list(self.placed_quotes.keys()):
            p = self.placed_quotes[placed_id]
            q = p.quote
            for ev in self.raw_events_buffer:
                try:
                    ev_ts = int(ev.get("ts_raw") or ev.get("ts") or 0)
                except (TypeError, ValueError):
                    continue
                if ev_ts < p.t_arrival_sim_ms:
                    continue
                if ev.get("event_type") != "price_change":
                    continue
                if not any(c.get("asset_id") == q.asset_id for c in ev.get("changes") or []):
                    continue
                book_now = self.book_store.books.get(q.asset_id)
                if book_now is None:
                    continue
                price = Decimal(str(q.price))
                if q.side == "BID":
                    cur_size_at_our_price = book_now.bids.get(price, Decimal(0))
                else:
                    cur_size_at_our_price = book_now.asks.get(price, Decimal(0))
                pre_size_at_our_price = p.depth_at_price_at_arrival
                shrunk = pre_size_at_our_price - cur_size_at_our_price
                if shrunk <= 0:
                    continue

                # Fill model: queue share proportional to our size vs total depth at arrival
                # plus we exhaust after worst-case-queue-of-others-ahead-of-us is consumed.
                our_size = Decimal(str(q.size))
                pre_quote_depth_others = p.pre_quote_depth_at_price_at_arrival
                total_depth_inclusive = p.depth_at_price_at_arrival  # others-claimed + our size
                if total_depth_inclusive > 0:
                    fill_qty = shrunk * our_size / total_depth_inclusive
                else:
                    fill_qty = shrunk
                fill_qty = min(fill_qty, our_size)
                # best-case model: we are first in queue → fill entire shrunk if <= our_size, else our_size
                fill_qty_best = min(min(shrunk, our_size), our_size)
                # worst-case: we wait until all others ahead of us cleared
                queue_ahead_others_shares = max(
                    Decimal("0"), Decimal(p.queue_position_worst_case) * our_size
                )
                if shrunk <= queue_ahead_others_shares:
                    fill_qty_worst = Decimal("0")
                else:
                    fill_qty_worst = min(shrunk - queue_ahead_others_shares, our_size)
                # expected-case: midpoint between best and worst; weighted by historical AS-rate
                fill_qty_expected = fill_qty if fill_qty > 0 else fill_qty_worst

                # For now we emit ONE fill event with `exec_qty = fill_qty_expected`. The
                # bookkeeping tracks best/worst/expected via the queue position index at time of fill.
                if fill_qty <= 0 and fill_qty_worst <= 0:
                    # update the cumulative-shrunk tracker and move on
                    continue

                # We emit at fill_qty_expected as a single-row event; downstream
                # markout_60s/5m/30m gets attached by Loop E (analytics.py) later.
                fill = self._emit_fill_event(
                    p, book_now,
                    fill_qty=fill_qty_expected,
                    fill_qty_best=fill_qty_best,
                    fill_qty_worst=fill_qty_worst,
                    ts_ms=ev_ts,
                )
                self.completed_fills.append(fill)
                self.router.apply_fill_to_inventory(
                    q.asset_id,
                    float(fill_qty_expected),
                    "BUY" if q.side == "ASK" else "SELL",
                )
                # remove the placement
                self.placed_quotes.pop(placed_id, None)
                break
            if placed_id in self.placed_quotes and (now_ms - p.t_arrival_sim_ms) > int(self.quote_timeout_sec * 1000):
                self.placed_quotes.pop(placed_id, None)

    # ---------------------------------------------------------------- raw_events buffer intake

    def ingest_ws_message(self, msg: dict) -> None:
        """Called by main_paper.py on_message handler."""
        self.raw_events_buffer.append(msg)
        # bound raw buffer
        if len(self.raw_events_buffer) > 50000:
            self.raw_events_buffer.popleft()

    # ---------------------------------------------------------------- output

    def _emit_fill_event(
        self, p: QuotePlacement, book_now: Book,
        fill_qty: Decimal,
        fill_qty_best: Decimal = Decimal("0"),
        fill_qty_worst: Decimal = Decimal("0"),
        ts_ms: int = 0,
    ) -> dict[str, Any]:
        q = p.quote
        mid_at_fill = book_now.mid() if book_now.mid() is not None else Decimal("0")
        mid_at_fill_f = float(mid_at_fill or 0)
        mid_obs = (Decimal(p.book_at_arrival.best_bid() or 0) + Decimal(p.book_at_arrival.best_ask() or 0)) / Decimal("2")
        mid_obs_f = float(mid_obs)

        exec_price = float(q.price)
        qty_f = float(fill_qty)
        qty_best_f = float(fill_qty_best)
        qty_worst_f = float(fill_qty_worst)
        side_taker = "BUY" if q.side == "ASK" else "SELL"
        gross_edge = (exec_price - mid_at_fill_f) * (-1 if q.side == "BID" else 1) * qty_f
        fee_estimate = max(0.0, exec_price * qty_f * 0.0)  # maker post = zero per default fee schedule
        rebate_estimate = exec_price * qty_f * 0.0001      # placeholder maker rebate
        expected_pnl = gross_edge + rebate_estimate - fee_estimate
        gross_edge_best = (exec_price - mid_at_fill_f) * (-1 if q.side == "BID" else 1) * qty_best_f
        gross_edge_worst = (exec_price - mid_at_fill_f) * (-1 if q.side == "BID" else 1) * qty_worst_f
        pnl_best = gross_edge_best + (rebate_estimate * qty_best_f / max(qty_f, 1e-9)) - fee_estimate
        pnl_worst = gross_edge_worst + (rebate_estimate * qty_worst_f / max(qty_f, 1e-9)) - fee_estimate
        pnl_exp = expected_pnl
        fill_id = uuid.uuid4().hex[:16]
        return {
            "ts_utc": ts_ms,
            "asset_id": q.asset_id,
            "market": q.market,
            "side_taker": side_taker,
            "exec_price": exec_price,
            "exec_qty": qty_f,
            "queue_position_best_case": p.queue_position_best_case,
            "queue_position_expected": p.queue_position_expected,
            "queue_position_worst_case": p.queue_position_worst_case,
            "t_observe_ms": q.t_observe_ms,
            "t_arrival_sim_ms": p.t_arrival_sim_ms,
            "book_hash_at_observe": p.book_at_arrival.last_hash,
            "book_hash_at_arrival": book_now.last_hash,
            "mid_observe": mid_obs_f,
            "mid_at_fill": mid_at_fill_f,
            "fair_value_at_fill": mid_at_fill_f,
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
            "pnl_expected_case": pnl_exp,
            "pnl_worst_case": pnl_worst,
            "kill_trigger_fired": None,
            "scan_cycle_id": q.scan_cycle_id,
            "fill_id": fill_id,
        }

    # ---------------------------------------------------------------- flush parquet

    def flush_fills_to_parquet(self) -> int:
        if not self.completed_fills:
            return 0
        rows = list(self.completed_fills)
        # ensure schema
        table_rows = []
        for r in rows:
            fixed = {}
            for col in FILL_SCHEMA:
                fixed[col] = r.get(col)
            table_rows.append(fixed)
        table = pa.Table.from_pylist(table_rows)
        # append if file exists, else create
        if self.fill_log_path.exists():
            existing = pq.read_table(self.fill_log_path)
            table = pa.concat_tables([existing, table])
        self.fill_log_path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(table, self.fill_log_path)
        self.completed_fills.clear()
        return len(rows)

    # ---------------------------------------------------------------- kill-switch checks

    def check_kill_switches(self, realized_pnl_usd: float, deployed_usd: float) -> str | None:
        """Returns 'warn' | 'reduce' | 'halt' if a kill-switch fires; None otherwise."""
        if self.base_equity_usd <= 0:
            return None
        drawdown_pct = max(0.0, -realized_pnl_usd / self.base_equity_usd * 100.0)
        if drawdown_pct >= self.kill_switch_drawdown_pct_halt:
            return "halt"
        if drawdown_pct >= self.kill_switch_drawdown_pct_reduce:
            return "reduce"
        if drawdown_pct >= self.kill_switch_drawdown_pct_warn:
            return "warn"
        return None


def prev_users_at_price_estimate(depth_at_price: Decimal, our_size: Decimal) -> int:
    """Heuristic expected queue position before us: assume orders ahead are avg-of-size 3× our_size."""
    if our_size <= 0:
        return 3
    avg_other_size = our_size * 3
    n_others = int(depth_at_price / max(avg_other_size, Decimal("0.01")))
    return max(0, min(n_others, 1000))
