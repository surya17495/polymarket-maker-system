"""poly_merger.py — sim-only merger for Phase 1A data: when both YES+NO are
held on the same condition_id at the same per-fill time, emit a MergeEvent that
returns (1 − p − q) USDC of capital to the deployable pool per share-pair merged.

p = our average buy price of YES share
q = our average buy price of NO  share
The locked edge = (1 − p − q) USDC per share pair.
Merging N pairs returns N×(1 − p − q) USDC.

In our offline lab simulator this is *pure event emission*: the router emits a
synthetic MergeEvent (as a ledger row), and an accounting hook records the
capital return to the deployable pool. No on-chain calls happen in Phase 1A.

Phase 2A will replace this with a real on-chain EIP-712 batch via the
builder-relayer path described in `poly_maker/merge.py` (EOA / Safe / V2
DepositWallet) — verified live on LeBron neg-risk merge tx 0x4d2a2064.
"""
from __future__ import annotations
import math
import uuid
from dataclasses import dataclass, field
from typing import Any


@dataclass
class MergeEvent:
    """One synthetic merge of `pair_qty` YES + pair_qty NO into `pair_qty` USDC."""
    condition_id: str
    pair_qty: float          # shares we can pair-and-merge
    avg_buy_yes_price: float
    avg_buy_no_price: float
    ts_merge: int            # wall-clock ms
    rebates_per_pair: float = 0.0  # maker rebate on the round-trip; default 0
    fill_id: str = ""
    scan_cycle_id: str = ""

    @property
    def locked_edge_per_pair(self) -> float:
        return max(0.0, 1.0 - self.avg_buy_yes_price - self.avg_buy_no_price)

    @property
    def capital_returned_usd(self) -> float:
        # 1 USDC per merged pair, less locked edge kept as realized PnL
        # The merge itself returns 1.0 USDC per pair; we paid (p + q) USDC for it.
        return float(self.pair_qty) * 1.0

    @property
    def realized_pnl_usd(self) -> float:
        # profit_km = pair_qty × (1 - p - q)  (the locked-edge spread; positive when both legs
        # were bought below fair value sum)
        return float(self.pair_qty) * max(0.0, 1.0 - self.avg_buy_yes_price - self.avg_buy_no_price)


@dataclass
class MarketInventory:
    """Tracker for YES+NO position on one condition_id — used by the merger."""
    condition_id: str
    yes_avg_cost: float = 0.0
    no_avg_cost: float = 0.0
    yes_shares: float = 0.0
    no_shares: float = 0.0
    realized_pnl: float = 0.0       # cumulative realized (sells + merges)
    last_merge_ts_ms: int = 0

    def apply_yes_fill(self, qty: float, price: float) -> None:
        if qty <= 0:
            return
        new_shares = self.yes_shares + qty
        self.yes_avg_cost = (self.yes_shares * self.yes_avg_cost + qty * price) / max(new_shares, 1e-9)
        self.yes_shares = new_shares

    def apply_no_fill(self, qty: float, price: float) -> None:
        if qty <= 0:
            return
        new_shares = self.no_shares + qty
        self.no_avg_cost = (self.no_shares * self.no_avg_cost + qty * price) / max(new_shares, 1e-9)
        self.no_shares = new_shares

    def apply_yes_sell(self, qty: float, price: float) -> None:
        if qty <= 0 or self.yes_shares <= 0:
            return
        sells = min(qty, self.yes_shares)
        self.realized_pnl += sells * (price - self.yes_avg_cost)
        self.yes_shares -= sells
        if self.yes_shares < 1e-9:
            self.yes_shares = 0.0
            self.yes_avg_cost = 0.0

    def apply_no_sell(self, qty: float, price: float) -> None:
        if qty <= 0 or self.no_shares <= 0:
            return
        sells = min(qty, self.no_shares)
        self.realized_pnl += sells * (price - self.no_avg_cost)
        self.no_shares -= sells
        if self.no_shares < 1e-9:
            self.no_shares = 0.0
            self.no_avg_cost = 0.0

    def try_merge(self, ts_ms: int, max_pairs: float | None = None) -> MergeEvent | None:
        """Pair up YES+NO inventories and return a MergeEvent (capital returned).
        
        Returns None if we can't pair anything (either side missing).
        """
        deg = min(self.yes_shares, self.no_shares)
        if max_pairs is not None:
            deg = min(deg, max_pairs)
        deg = math.floor(deg * 100) / 100  # 2-decimal truncation to match Polymarket
        if deg <= 0:
            return None
        ev = MergeEvent(
            condition_id=self.condition_id,
            pair_qty=deg,
            avg_buy_yes_price=self.yes_avg_cost,
            avg_buy_no_price=self.no_avg_cost,
            ts_merge=ts_ms,
            fill_id=uuid.uuid4().hex[:16],
        )
        self.yes_shares -= deg
        self.no_shares -= deg
        if self.yes_shares < 1e-9:
            self.yes_shares = 0.0
            self.yes_avg_cost = 0.0
        if self.no_shares < 1e-9:
            self.no_shares = 0.0
            self.no_avg_cost = 0.0
        self.realized_pnl += ev.realized_pnl_usd
        self.last_merge_ts_ms = ts_ms
        return ev


@dataclass
class MergerState:
    """Cross-market book keeping for the YES+NO position merger."""
    by_condition: dict[str, MarketInventory] = field(default_factory=dict)

    def get(self, condition_id: str) -> MarketInventory:
        if condition_id not in self.by_condition:
            self.by_condition[condition_id] = MarketInventory(condition_id=condition_id)
        return self.by_condition[condition_id]

    def get_by_token(self, token_to_condition: dict[str, str], token_id: str) -> MarketInventory | None:
        cid = token_to_condition.get(token_id)
        if cid is None:
            return None
        return self.get(cid)

    def record_fill(self, token_to_condition: dict[str, str], token_id: str, qty: float, price: float, side_taker: str, ts_ms: int) -> None:
        inv = self.get_by_token(token_to_condition, token_id)
        if inv is None:
            return
        # `side_taker` here is "BUY" if the taker bought our SELL / lifted our ASK (we SOLD)
        # and "SELL" if the taker sold into our BID (we BOUGHT)
        # We compute "we are buying" vs "we are selling":
        we_buy_yes = side_taker == "SELL" and token_id != ""  # taker SOLD into our BID → we bought
        we_sell_yes = side_taker == "BUY" and token_id != ""  # taker BOUGHT our ASK → we sold
        is_yes_token = (token_id == inv.condition_id) or (token_id.endswith(_last_hex(inv.condition_id)))

        # Determine which side is YES vs NO via the simpler convention: YES is the
        # first token_id in a self-consistent way. In the lab, we feed token_kind
        # ("YES" | "NO") from the caller; here we rely on the lab to pass that.
        # Simpler record: cb caller passes token_kind and we routed + notation
        # of "side_we_took". (See `record_fill_v2` below for the canonical call.)
        pass

    def record_fill_v2(
        self,
        condition_id: str,
        token_kind: str,           # "YES" | "NO"
        qty: float,
        price: float,
        we_bought: bool,           # True if WE BOUGHT (taker hit our BID); False if WE SOLD (taker lifted our ASK)
    ) -> None:
        inv = self.get(condition_id)
        if we_bought:
            if token_kind == "YES":
                inv.apply_yes_fill(qty, price)
            else:
                inv.apply_no_fill(qty, price)
        else:
            if token_kind == "YES":
                inv.apply_yes_sell(qty, price)
            else:
                inv.apply_no_sell(qty, price)

    def try_merge_all(self, ts_ms: int, max_pairs_per_market: float | None = None) -> list[MergeEvent]:
        events: list[MergeEvent] = []
        for cid, inv in self.by_condition.items():
            ev = inv.try_merge(ts_ms, max_pairs=max_pairs_per_market)
            if ev is not None:
                events.append(ev)
        return events

    @property
    def total_realized_pnl(self) -> float:
        return sum(inv.realized_pnl for inv in self.by_condition.values())


def _last_hex(condition_id: str) -> str:
    if not condition_id:
        return ""
    return condition_id[-6:]
