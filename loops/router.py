"""router.py — Loop B (Phase 1A): state machine that decides quote_submit events.

Refactored to support a pluggable Strategy interface per asset_id. Default strategy
is BBTickStrategy (S0) — same behavior as the original BB+tick + inventory-lean
heuristic, emitting ONE QuoteSubmit per asset per tick. New strategies inherit
Strategy and emit zero, one, OR MORE QuoteSubmits per tick (e.g. two-sided quoting).
The Strategy Lab (lib/strategy_lab.py) runs multiple strategies offline against
the same captured raw_events.jsonl to compare apples-to-apples.

Backward compatibility: the existing `Router._calc_quote` keeps returning a single
QuoteSubmit|None (used by old tests that bypass decide_quote_submits). The new
`Router.decide_quote_submits` now delegates to `strategy.quote_at_tick(...)` and
flattens the list of QuoteSubmits returned.
"""
from __future__ import annotations
import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from lib.book import BookStore, Book
from yaml import safe_load

log = logging.getLogger(__name__)


@dataclass
class QuoteSubmit:
    """A pending quote the paper executor simulates."""
    asset_id: str
    market: str
    side: str  # "BID" | "ASK"
    price: float
    size: float  # shares
    t_observe_perf_counter: float
    t_observe_ms: int  # wall clock at t_observe
    scan_cycle_id: str


@dataclass
class InventoryState:
    per_market: dict[str, float] = field(default_factory=dict)  # asset_id -> signed share count
    per_market_usd: dict[str, float] = field(default_factory=dict)  # asset_id -> |inventory| * mid

    def net_shares(self, asset_id: str) -> float:
        return self.per_market.get(asset_id, 0.0)

    def net_inventory_usd(self, asset_id: str) -> float:
        return self.per_market_usd.get(asset_id, 0.0)

    def total_inventory_usd(self) -> float:
        return sum(self.per_market_usd.values())

    def apply_fill(self, asset_id: str, fill_qty: float, side_taker: str, mid: float) -> None:
        if side_taker == "SELL":
            self.per_market[asset_id] = self.per_market.get(asset_id, 0.0) - fill_qty
        else:
            self.per_market[asset_id] = self.per_market.get(asset_id, 0.0) + fill_qty
        self.per_market_usd[asset_id] = abs(self.per_market[asset_id]) * mid


@dataclass
class RouterConfig:
    quote_size_usd: float = 50.0
    max_inventory_per_market_usd: float = 50.0
    max_total_inventory_usd: float = 200.0
    max_quote_lag_ms: int = 300_000  # documented in strategy doc §4
    target_inventory_per_market_usd: float = 0.0
    tick_size_default: float = 0.001
    tick_improvement_only: bool = True
    router_tick_sec: int = 60  # configurable; default 60 per doc §4, §13


class Strategy(ABC):
    """Pluggable quote-emission strategy for a single asset.

    Called once per asset per router tick with the current book state + inventory.
    Returns a list of QuoteSubmit events (zero, one, two, ...).

    Each strategy is identified by `strategy_id` which the Strategy Lab uses to
    namespace per-strategy ledgers + summaries.
    """
    strategy_id: str = "base_strategy"

    @abstractmethod
    def quote_at_tick(
        self,
        book: Book,
        asset_id: str,
        inv: InventoryState,
        cfg: RouterConfig,
        params: dict,
        now_ms: int,
    ) -> list[QuoteSubmit]:
        ...


class BBTickStrategy(Strategy):
    """S0 — original BB+tick heuristic preserved as the default strategy.

    Emits ONE QuoteSubmit per asset per tick. Default side = BID when flat
    (_even_book_side = "BID"); flips to ASK when long, BID when short.
    """
    strategy_id: str = "s0_bb_tick"

    def quote_at_tick(self, book, asset_id, inv, cfg, params, now_ms):
        bb = book.best_bid()
        ba = book.best_ask()
        if bb is None or ba is None:
            return []
        mid_f = float((bb + ba) / 2)
        if mid_f <= 0:
            return []
        if book.last_update_ms <= 0 or (now_ms - book.last_update_ms) > cfg.max_quote_lag_ms:
            return []
        inv_usd = inv.net_inventory_usd(asset_id)
        if inv_usd >= cfg.max_inventory_per_market_usd:
            return []
        if inv.total_inventory_usd() >= cfg.max_total_inventory_usd:
            return []
        quote_size = cfg.quote_size_usd / mid_f
        tick = float(book.tick_size) or cfg.tick_size_default
        net = inv.net_shares(asset_id)
        if net > 0:
            side = "ASK"
            price = float(ba) - tick if (float(ba) - tick) > float(bb) else float(ba)
        elif net < 0:
            side = "BID"
            price = float(bb) + tick if (float(bb) + tick) < float(ba) else float(bb)
        else:
            side = "BID"
            price = float(bb) + tick if (float(bb) + tick) < float(ba) else float(bb)
        perf_t = params.get("now_perf", time.perf_counter())
        return [QuoteSubmit(
            asset_id=asset_id, market=book.market, side=side,
            price=price, size=quote_size,
            t_observe_perf_counter=perf_t, t_observe_ms=now_ms,
            scan_cycle_id=params.get("scan_id", "tick"),
        )]


@dataclass
class Router:
    cfg: RouterConfig
    book_store: BookStore
    inventory: InventoryState = field(default_factory=InventoryState)
    strategy: Strategy | None = None

    def __post_init__(self):
        if self.strategy is None:
            self.strategy = BBTickStrategy()

    def _calc_quote(
        self,
        asset_id: str,
        book: Book,
        scan_id: str,
        now_perf: float,
        now_ms: int,
    ) -> QuoteSubmit | None:
        """Backward-compat: return a single QuoteSubmit|None.

        Used by the existing test_paper_executor.py — pulls the FIRST emit from
        the strategy. The new path `decide_quote_submits` flattens to a list.
        """
        out = self.strategy.quote_at_tick(
            book, asset_id, self.inventory, self.cfg,
            {"scan_id": scan_id, "now_perf": now_perf},
            now_ms,
        )
        return out[0] if out else None

    def decide_quote_submits(
        self,
        scan_id: str,
        watch_assets: list[str],
        now_ms: int | None = None,
        now_perf: float | None = None,
    ) -> list[QuoteSubmit]:
        """Use the strategy interface directly; emit a flattened list of QuoteSubmits.
        
        now_ms / now_perf may be supplied by an offline-replay lab to drive simulated
        time forward in sync with the event stream; defaults to real wall-clock.
        """
        if now_ms is None:
            now_ms = int(time.time() * 1000)
        if now_perf is None:
            now_perf = time.perf_counter()
        out: list[QuoteSubmit] = []
        no_book = 0
        no_bbo = 0
        stale = 0
        inv_full = 0
        params: dict[str, Any] = {"scan_id": scan_id, "now_perf": now_perf, "now_ms": now_ms}
        for aid in watch_assets:
            b = self.book_store.books.get(aid)
            if b is None:
                no_book += 1
                continue
            qs = self.strategy.quote_at_tick(b, aid, self.inventory, self.cfg, params, now_ms)
            if not qs:
                if b.best_bid() is None or b.best_ask() is None:
                    no_bbo += 1
                elif (now_ms - b.last_update_ms) > self.cfg.max_quote_lag_ms:
                    stale += 1
                else:
                    inv_full += 1
                continue
            out.extend(qs)
        log.debug(
            "router tick scan_id=%s: watch=%d no_book=%d no_bbo=%d stale=%d inv_full=%d emitted=%d",
            scan_id, len(watch_assets), no_book, no_bbo, stale, inv_full, len(out)
        )
        return out

    def apply_fill_to_inventory(
        self, asset_id: str, fill_qty: float, side_taker: str
    ) -> None:
        book = self.book_store.books.get(asset_id)
        mid = float(book.mid()) if book and book.mid() is not None else 0.0
        self.inventory.apply_fill(asset_id, fill_qty, side_taker, mid)


def _even_book_side(best_bid: Decimal, best_ask: Decimal) -> str:
    return "BID"


def load_router_config_from_yaml(path: str) -> RouterConfig:
    with open(path) as f:
        y = safe_load(f)
    kill = y.get("kill_switches") or {}
    scan = y.get("scanner") or {}
    router_sec = y.get("router") or {}
    return RouterConfig(
        max_inventory_per_market_usd=float(kill.get("max_inventory_per_market_usd", 50)),
        max_total_inventory_usd=float(kill.get("max_total_inventory_usd", 200)),
        max_quote_lag_ms=int(kill.get("max_quote_lag_ms", 800)),
        quote_size_usd=float(y.get("allocator", {}).get("per_market_cap_usd_phase1a", 50)),
        tick_size_default=float(scan.get("min_spread_c", 0.5) / 100.0),
        router_tick_sec=int(router_sec.get("tick_sec", 60)),
    )
