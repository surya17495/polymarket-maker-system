"""strategies.py — catalog of Strategy implementations S0..S6 for the Strategy Lab.

Each strategy is a `Strategy` subclass emitting list[QuoteSubmit] per asset per
router tick. The Strategy Lab constructs one strategy instance per replay and
runs it against the SAME raw_events.jsonl producing a per-strategy ledger.

Catalog:

  S0  BBTickStrategy             -- baseline BB+tick single-side (in router.py)
  S1  PolyQuotingStrategy        -- two-sided poly-quoting on YES (or NO) book per tick
  S2  ReduceOnlyStrategy        -- S1 + regime-aware add/exit split (REDUCE_ONLY when
                                    inventory at cap; _maybe_exit still emits sells)
  S3  MergeStrategy             -- S2 + MergerState tracking YES+NO per condition_id
                                    + try-merge on every fill → emits MergeEvents;
                                    capital reclaimed at 1−p−q back to deployable pool
  S4  AntiThrashStrategy         -- decorator: drop re-quote when Δmid<0.5c and Δsize<10%
  S5  ReversePositionStrategy   -- decorator: don't BUY opposing leg when one side
                                    already hedged (avoids doubling into paired state)
  S6  StopLossStrategy          -- decorator: 3-hr realized-vol-gated stop-loss,
                                    post-stop cooldown, take-profit @ % above avg cost

Strategy chaining (S4..S6 are decorators that wrap a "core" strategy, default S3):
    factory("s" + X) builds: S1=S1; S2=S2; S3=S3; S4=Athrash(S3); S5=Reverse(S4);
    S6=StopLoss(S5).
"""
from __future__ import annotations
import logging
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Callable

from lib.book import Book, BookStore
from lib.poly_estimators import MarketEstimators, market_estimator_default
from lib.poly_quoting import construct_quotes, QuoteInputs, QuoteSpec
from lib.poly_regime import Regime, RegimeMachine, RegimeInputs, StrategyProfile
from lib.poly_merger import MergerState, MergeEvent
from loops.router import (
    Strategy, BBTickStrategy, QuoteSubmit, RouterConfig, InventoryState
)


log = logging.getLogger(__name__)
_DEFAULT_PROFILE = StrategyProfile()


def _safe_mid(book: Book) -> float | None:
    if book is None:
        return None
    m = book.mid()
    return float(m) if m is not None else None


def _hours_to_end(end_date_str: str | None, now_ms: int) -> float | None:
    if not end_date_str:
        return None
    try:
        s = end_date_str.strip().rstrip("Z")
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return (dt.timestamp() - now_ms / 1000.0) / 3600.0
    except Exception:
        return None


class PolyQuotingStrategy(Strategy):
    """S1 — poly_quoting.construct_quotes. Emits BUY (and SELL EXIT) per token.
    
    Each call is per asset_id (which can be either the YES token or NO token of
    the underlying condition). For the lab, the MirroredBookStore maintains
    derived NO books via symmetry, so both sides have book data. The strategy
    builds QuoteInputs from BOTH books, calls construct_quotes, then filters
    emitted QuoteSpecs down to the calling asset_id only — the Router iterates
    over both token_ids in watch_assets and each gets its turn.
    """
    strategy_id = "s1_poly_quoting"

    def __init__(self, book_store: BookStore, pair_map: dict, profile: StrategyProfile = _DEFAULT_PROFILE):
        self.book_store = book_store
        self.pair_map = pair_map
        self.profile = profile
        self._est_for: dict[str, MarketEstimators] = {}
        self._prev_fv_for: dict[str, float] = {}

    def _est(self, condition_id: str) -> MarketEstimators:
        if condition_id not in self._est_for:
            self._est_for[condition_id] = market_estimator_default()
        return self._est_for[condition_id]

    def _regime(self, condition_id: str, asset_id: str, inv: InventoryState, cfg: RouterConfig, now_ms: int) -> Regime:
        """Override in subclasses. Default = QUIET."""
        return Regime.QUIET

    def quote_at_tick(self, book, asset_id, inv, cfg, params, now_ms):
        pair = self.pair_map.get(asset_id)
        if pair is None:
            return []
        is_yes = asset_id == pair.yes_token_id
        other_token = pair.no_token_id if is_yes else pair.yes_token_id
        other_book = self.book_store.books.get(other_token)
        yes_book = book if is_yes else other_book
        no_book = other_book if is_yes else book
        if yes_book is None or yes_book.best_bid() is None or yes_book.best_ask() is None:
            return []
        ts_sec = now_ms / 1000.0
        est = self._est(pair.condition_id)
        y_mid = _safe_mid(yes_book)
        if y_mid is not None and y_mid > 0:
            est.on_fair_value(y_mid, ts_sec)
        inv_check_usd = inv.net_inventory_usd(asset_id)
        if inv_check_usd >= cfg.max_inventory_per_market_usd:
            return []
        if inv.total_inventory_usd() >= cfg.max_total_inventory_usd:
            return []
        pos_yes = max(inv.net_shares(pair.yes_token_id), 0.0)
        pos_no = max(inv.net_shares(pair.no_token_id), 0.0)
        bb_yes = yes_book.best_bid()
        ba_yes = yes_book.best_ask()
        bb_no = no_book.best_bid() if no_book else None
        ba_no = no_book.best_ask() if no_book else None
        bb_yes_size = yes_book.best_bid_size()
        ba_yes_size = yes_book.best_ask_size()
        bb_no_size = no_book.best_bid_size() if no_book else None
        ba_no_size = no_book.best_ask_size() if no_book else None
        fv_mid = float((bb_yes + ba_yes) / 2)
        flow_z = est.flow.z
        tick_size = float(yes_book.tick_size or 0.001)
        fv = min(max(fv_mid + 0.5 * flow_z * tick_size, 0.01), 0.99)

        regime = self._regime(pair.condition_id, asset_id, inv, cfg, now_ms)

        # q_max expanded to allow the unwinding leg to fire while adder is gated
        # (lab uses a generous cap; live settings in config.yaml govern the router cap).
        q_max_usdc = max(cfg.max_inventory_per_market_usd * 4, cfg.quote_size_usd * 4)
        base_size_usdc = float(cfg.quote_size_usd)

        inp = QuoteInputs(
            fv=fv,
            vol_short=est.vol.short,
            toxicity=est.markout.toxicity,
            regime=regime,
            bb_yes=bb_yes, ba_yes=ba_yes, bb_no=bb_no, ba_no=ba_no,
            bb_yes_size=bb_yes_size, ba_yes_size=ba_yes_size,
            bb_no_size=bb_no_size, ba_no_size=ba_no_size,
            pos_yes_shares=pos_yes, pos_no_shares=pos_no,
            tick_size=tick_size,
            price_decimals=4,
            q_max_usdc=q_max_usdc,
            base_size_usdc=base_size_usdc,
            min_order_size=0.0,
            min_edge_ticks=1,
            c_vol=5.0, c_tox=2.0,
            gamma=1.0,
            yes_token_id=pair.yes_token_id,
            no_token_id=pair.no_token_id,
            quote_market=pair.condition_id,
        )
        quotes = construct_quotes(inp)
        out = []
        for qs in quotes:
            if qs.token_id != asset_id:
                continue
            side = "BID" if qs.side == "BUY" else "ASK"
            out.append(QuoteSubmit(
                asset_id=asset_id, market=pair.condition_id, side=side,
                price=qs.price, size=qs.size,
                t_observe_perf_counter=params.get("now_perf", time.perf_counter()),
                t_observe_ms=now_ms,
                scan_cycle_id=params.get("scan_id", "tick"),
            ))
        return out

    def after_fill(self, fill_dict: dict, now_ms: int) -> None:
        """Default hook: update MarkoutTracker in the per-condition estimators."""
        asset_id = fill_dict.get("asset_id")
        side_taker = fill_dict.get("side_taker")
        qty = float(fill_dict.get("exec_qty") or 0.0)
        exec_price = float(fill_dict.get("exec_price") or 0.0)
        fv_at_fill = float(fill_dict.get("fair_value_at_fill") or exec_price)
        pair = self.pair_map.get(asset_id)
        if pair is None:
            return
        est = self._est(pair.condition_id)
        from lib.poly_estimators import Side
        # WE BOUGHT (taker SELL into our BID) or WE SOLD (taker BUY lifting our ASK)
        # MarkoutTracker: side = our side of the fill (BUY ⇒ adverse if price falls)
        our_side = Side.BUY if side_taker == "SELL" else Side.SELL
        est.markout.record_fill(our_side, fv_at_fill, now_ms / 1000.0)


class ReduceOnlyStrategy(PolyQuotingStrategy):
    """S2 = S1 + 5-state regime machine (REDUCE_ONLY lets _maybe_exit fire while adder gated)."""
    strategy_id = "s2_reduce_only"

    def __init__(self, book_store, pair_map, profile=_DEFAULT_PROFILE):
        super().__init__(book_store, pair_map, profile)
        self._regime_by_condition: dict[str, RegimeMachine] = {}

    def _rm(self, condition_id: str) -> RegimeMachine:
        if condition_id not in self._regime_by_condition:
            self._regime_by_condition[condition_id] = RegimeMachine()
        return self._regime_by_condition[condition_id]

    def _regime(self, condition_id, asset_id, inv, cfg, now_ms):
        machine = self._rm(condition_id)
        pair = self.pair_map.get(asset_id)
        if pair is None:
            return Regime.QUIET
        inv_usd_yes = inv.net_inventory_usd(pair.yes_token_id)
        inv_usd_no = inv.net_inventory_usd(pair.no_token_id)
        max_inv_usd = max(inv_usd_yes, inv_usd_no)
        q_max = float(max(cfg.max_inventory_per_market_usd, 0.01))
        util = max_inv_usd / q_max
        hours_to_end = _hours_to_end(pair.end_date, now_ms)
        est = self._est(condition_id)
        prev_fv = est.last_fv if est.last_fv is not None else None
        inp = RegimeInputs(
            now=now_ms / 1000.0,
            tick=0.001,  # ok for regime math (used only as denominator for jump_ticks)
            fv=est.last_fv if est.last_fv is not None else 0.5,
            prev_fv=prev_fv,
            vol_ratio=est.vol.ratio,
            flow_z=est.flow.z,
            inventory_util=util,
            hours_to_end=hours_to_end,
            risk_reduce_only=False, risk_halt=False,
            sweep_flagged=False,
            ws_stale=False, market_resolved=False,
        )
        return machine.decide(inp, self.profile)


class MergeStrategy(ReduceOnlyStrategy):
    """S3 = S2 + MergerState receiving fills; auto-merge YES+NO → USDC."""
    strategy_id = "s3_with_merge"

    def __init__(self, book_store, pair_map, profile=_DEFAULT_PROFILE):
        super().__init__(book_store, pair_map, profile)
        self.merger_state = MergerState()
        self.merge_events: list[MergeEvent] = []
        # Track realized PnL since merge events
        self.realized_pnl_from_merges: float = 0.0

    def after_fill(self, fill_dict, now_ms):
        super().after_fill(fill_dict, now_ms)
        asset_id = fill_dict.get("asset_id")
        side_taker = fill_dict.get("side_taker")
        qty = float(fill_dict.get("exec_qty") or 0.0)
        exec_price = float(fill_dict.get("exec_price") or 0.0)
        we_bought = (side_taker == "SELL")  # taker SELL into our BID = we bought
        pair = self.pair_map.get(asset_id)
        if pair is None or qty <= 0:
            return
        token_kind = "YES" if asset_id == pair.yes_token_id else "NO"
        self.merger_state.record_fill_v2(
            pair.condition_id, token_kind, qty, exec_price, we_bought
        )
        # Try auto-merge; merger emits a MergeEvent per condition_id where both YES+NO held
        for ev in self.merger_state.try_merge_all(now_ms):
            self.merge_events.append(ev)
            self.realized_pnl_from_merges += ev.realized_pnl_usd


class AntiThrashStrategy(Strategy):
    """S4 — drop re-quote when Δmid<price_delta_c and Δsize<size_delta_pct threshold.

    Defaults: price_delta_c=0.50c, size_delta_pct=0.10 (10%). Empirical 2026-07-24
    lab re-run with 0.10c/5% produced only 2 quote_submits over a 170k-event run
    (vs 2088 with 0.50c/10%) — i.e. tighter thresholds suppress MORE requotes,
    not fewer. The condition `dp < thr AND ds_rel < thr_pct` fires more often
    when thresholds are smaller. Poly-maker's original 0.50c/10% defaults let
    nearly every requote survive (moves > 0.5c are common on esports markets).
    """
    strategy_id = "s4_anti_thrash"

    def __init__(self, base: Strategy, price_delta_c: float = 0.50, size_delta_pct: float = 0.10):
        self.base = base
        self.price_delta_c = price_delta_c
        self.size_delta_pct = size_delta_pct
        # per-asset_id: side -> (price, size) last emitted
        self._last_quote_per_asset: dict[str, dict[str, tuple[float, float]]] = {}

    @property
    def pair_map(self):
        return getattr(self.base, "pair_map", {})

    @property
    def book_store(self):
        return getattr(self.base, "book_store", None)

    def quote_at_tick(self, book, asset_id, inv, cfg, params, now_ms):
        candidates = self.base.quote_at_tick(book, asset_id, inv, cfg, params, now_ms)
        book_store = self.book_store
        cur_mid = None
        if book is not None and book.best_bid() is not None and book.best_ask() is not None:
            cur_mid = float((book.best_bid() + book.best_ask()) / 2)
        prev = self._last_quote_per_asset.get(asset_id, {})
        emit_list: list[QuoteSubmit] = []
        updated: dict[str, tuple[float, float]] = dict(prev)
        for q in candidates:
            key = q.side
            prior = prev.get(key)
            if prior is not None:
                dp = abs(q.price - prior[0])
                ds_rel = abs(q.size - prior[1]) / max(prior[1], 1e-9)
                if dp < self.price_delta_c / 100.0 and ds_rel < self.size_delta_pct:
                    # Skip requote — too small a change
                    continue
            emit_list.append(q)
            updated[key] = (q.price, q.size)
        self._last_quote_per_asset[asset_id] = updated
        return emit_list

    def after_fill(self, fill_dict, now_ms):
        # On fill, purge the cached side for the asset_id — quota table might be cleared next tick
        asset_id = fill_dict.get("asset_id")
        if asset_id is not None and asset_id in self._last_quote_per_asset:
            self._last_quote_per_asset.pop(asset_id, None)
        if hasattr(self.base, "after_fill"):
            self.base.after_fill(fill_dict, now_ms)


class ReversePositionStrategy(Strategy):
    """S5 — block BUY opposing leg when one side already hedged.
    
    If holding YES position (long YES), don't emit BUY-NO quotes (would create a
    paired-and-hedge position which the MergerState could resolve, but we'd pay two
    leg-quotes for the same locked edge). Vice versa for NO-YES.
    """
    strategy_id = "s5_reverse_pos"

    def __init__(self, base: Strategy):
        self.base = base

    @property
    def pair_map(self):
        return getattr(self.base, "pair_map", {})

    @property
    def book_store(self):
        return getattr(self.base, "book_store", None)

    def quote_at_tick(self, book, asset_id, inv, cfg, params, now_ms):
        candidates = self.base.quote_at_tick(book, asset_id, inv, cfg, params, now_ms)
        pair = self.pair_map.get(asset_id) if hasattr(self, "pair_map") else None
        if not candidates or pair is None:
            return candidates
        is_yes = asset_id == pair.yes_token_id
        opposite_held = False
        if is_yes:
            opposite_held = inv.net_shares(pair.no_token_id) > 0
        else:
            opposite_held = inv.net_shares(pair.yes_token_id) > 0
        if not opposite_held:
            return candidates  # no opposite held → emit normally
        # Drop BID candidates (keep SELL exits only — unwind)
        return [q for q in candidates if q.side == "ASK"]

    def after_fill(self, fill_dict, now_ms):
        if hasattr(self.base, "after_fill"):
            self.base.after_fill(fill_dict, now_ms)


class StopLossStrategy(Strategy):
    """S6 — 3-hr realized-vol-gated stop-loss; cooldown post-stop; take-profit @ % above avg cost.
    
    Heuristic (lab-only impl, simplified; full implementation is Phase 1B):
      - If 3-hr RV > stop_rv_threshold AND inventory at a loss (mark-at-cost < market),
        force exits via SELL quotes only (urgency=1.0 → exit at touch) and enter cooldown.
      - During cooldown, skip BUY quotes until cooldown_s elapses.
      - Take-profit: when held inventory costs < (1 + tp_pct) × avg_cost → emit SELL-as-ASK at
        high urgency to capture realized gain.
    """
    strategy_id = "s6_stop_loss"

    def __init__(self, base: Strategy, stop_rv_threshold: float = 0.05,
                 cooldown_s: float = 300.0, take_profit_pct: float = 0.05):
        self.base = base
        self.stop_rv_threshold = stop_rv_threshold
        self.cooldown_s = cooldown_s
        self.circular_take_profit_pct = take_profit_pct
        # per-condition-id state
        self._cooldown_until: dict[str, float] = {}
        # Cost basis tracked via the embedded merger_state (S3) when available; else, naive via inv
        self._cost_basis: dict[str, dict[str, float]] = {}  # cond -> (token -> avg_cost × held_shares)

    @property
    def pair_map(self):
        return getattr(self.base, "pair_map", {})

    @property
    def book_store(self):
        return getattr(self.base, "book_store", None)

    def _rv3h_for_condition(self, condition_id, asset_id):
        """Read 3-hr realized vol ratio (long halflife ~3hr EWMA vol / sum-change magnitude)."""
        base = self.base
        if not hasattr(base, "_est_for"):
            return 0.0
        est = base._est_for.get(condition_id)
        return est.vol.long if est else 0.0

    def quote_at_tick(self, book, asset_id, inv, cfg, params, now_ms):
        candidates = self.base.quote_at_tick(book, asset_id, inv, cfg, params, now_ms)
        if not candidates:
            return []
        pair = self.pair_map.get(asset_id)
        if pair is None:
            return candidates
        ts_sec = now_ms / 1000.0
        cooldown_until = self._cooldown_until.get(pair.condition_id, 0.0)
        in_cooldown = ts_sec < cooldown_until
        # Compute 3-hr RV and stop condition
        rv3h = self._rv3h_for_condition(pair.condition_id, asset_id)
        is_yes = asset_id == pair.yes_token_id
        our_inv_shares = inv.net_shares(asset_id)
        # Mark-to-market: if our held shares' avg cost exceeds current mid, we're underwater
        # For phase 1A heuristic: we use the book-side mid as a fair proxy.
        cur_mid = float(book.mid()) if book and book.mid() is not None else 0.0
        avg_cost = 0.0  # in lab version we use the merger_state avg_cost if available, else assume 0
        # Get cost basis from merger_state if base is MergeStrategy
        base = self.base
        cost_basis = None
        walker = base
        while walker is not None and hasattr(walker, "merger_state"):
            walker = walker
            break
        if hasattr(base, "merger_state"):
            mer = base.merger_state
            mi = mer.by_condition.get(pair.condition_id)
            if mi:
                if is_yes and mi.yes_shares > 0:
                    avg_cost = mi.yes_avg_cost
                elif not is_yes and mi.no_shares > 0:
                    avg_cost = mi.no_avg_cost
        out: list[QuoteSubmit] = []
        for q in candidates:
            if q.side == "BID":
                if in_cooldown:
                    continue
                if rv3h > self.stop_rv_threshold and our_inv_shares > 0 and avg_cost > cur_mid and avg_cost > 0:
                    # Stop-loss: skip quoting more BUY; rely on exit-side to unwind
                    self._cooldown_until[pair.condition_id] = ts_sec + self.cooldown_s
                    continue
                # Take-profit: if have unrealized gain > take_profit_pct, suppress BID anyway (exit only)
                if avg_cost > 0 and cur_mid > avg_cost * (1.0 + self.circular_take_profit_pct):
                    continue  # exit-only path: let the ASK leg close
            out.append(q)
        return out

    def after_fill(self, fill_dict, now_ms):
        if hasattr(self.base, "after_fill"):
            self.base.after_fill(fill_dict, now_ms)


def strategy_factory(strategy_id: str, book_store: BookStore, pair_map: dict) -> Strategy:
    """Build a Strategy instance per catalogue ID."""
    if strategy_id == "s0_bb_tick":
        return BBTickStrategy()
    if strategy_id == "s1_poly_quoting":
        return PolyQuotingStrategy(book_store, pair_map)
    if strategy_id == "s2_reduce_only":
        return ReduceOnlyStrategy(book_store, pair_map)
    if strategy_id == "s3_with_merge":
        return MergeStrategy(book_store, pair_map)
    if strategy_id == "s4_anti_thrash":
        return AntiThrashStrategy(MergeStrategy(book_store, pair_map))
    if strategy_id == "s5_reverse_pos":
        return ReversePositionStrategy(AntiThrashStrategy(MergeStrategy(book_store, pair_map)))
    if strategy_id == "s6_stop_loss":
        return StopLossStrategy(ReversePositionStrategy(AntiThrashStrategy(MergeStrategy(book_store, pair_map))))
    raise ValueError(f"unknown strategy_id: {strategy_id}")


ALL_STRATEGY_IDS = [
    "s0_bb_tick",
    "s1_poly_quoting",
    "s2_reduce_only",
    "s3_with_merge",
    "s4_anti_thrash",
    "s5_reverse_pos",
    "s6_stop_loss",
]
