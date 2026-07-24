"""book.py — Polymarket CLOB FIFO L2 book reconstruction.

Maintains per-asset_id limit-order-books (one Decimal price -> Decimal size map
per side). Applies `book` snapshots (full state) and `price_change` deltas
(new size at price; size == 0 removes level).

Polymarket convention:
  - side=="BUY"  = bid side (price at which buyers are willing to buy shares)
  - side=="SELL" = ask side
  - prices are tick_size increments (spec'd per market, often 0.001 / 0.01)
  - size = shares at that price; payout = $1 per share on YES outcome

Snapshot messages wrap Polymarket WS `book` event. Delta messages wrap
Polymarket WS `price_change` event (a list of changes under `price_changes`).
"""
from __future__ import annotations
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Iterable


def _dec(v) -> Decimal:
    if v is None:
        return Decimal(0)
    if isinstance(v, Decimal):
        return v
    s = str(v).strip()
    return Decimal(s) if s else Decimal(0)


def _to_levels(arr: Iterable[dict]) -> dict[Decimal, Decimal]:
    out: dict[Decimal, Decimal] = {}
    for e in arr or []:
        p = _dec(e.get("price"))
        s = _dec(e.get("size"))
        if p > 0 and s > 0:
            out[p] = s
    return out


@dataclass
class Book:
    asset_id: str
    market: str = ""
    tick_size: Decimal = Decimal("0.001")
    bids: dict[Decimal, Decimal] = field(default_factory=dict)
    asks: dict[Decimal, Decimal] = field(default_factory=dict)
    last_hash: str | None = None
    last_update_ms: int = 0
    last_ltp: Decimal = Decimal(0)

    def apply_snapshot(self, msg: dict) -> None:
        self.bids = _to_levels(msg.get("bids", []))
        self.asks = _to_levels(msg.get("asks", []))
        self.last_hash = msg.get("hash")
        # Use local recv_t_ms if present (our capture time) — falls back to server ts_raw
        ts = msg.get("recv_t_ms") or msg.get("ts_raw") or msg.get("ts") or msg.get("timestamp")
        try:
            self.last_update_ms = int(ts) if ts is not None else 0
        except (TypeError, ValueError):
            self.last_update_ms = 0
        ltp = msg.get("last_trade_price")
        if ltp is not None:
            self.last_ltp = _dec(ltp)

    def apply_change(self, change: dict, ts_ms: int | None = None) -> None:
        p = _dec(change.get("price"))
        size = _dec(change.get("size"))
        side = change.get("side") or "BUY"
        book_side = self.bids if side == "BUY" else self.asks
        if p <= 0:
            return
        if size <= 0:
            book_side.pop(p, None)
        else:
            book_side[p] = size
        if "hash" in change and change.get("hash"):
            self.last_hash = change["hash"]
        if ts_ms is not None:
            try:
                self.last_update_ms = int(ts_ms)
            except (TypeError, ValueError):
                pass

    def apply_price_change_msg(self, msg: dict) -> int:
        """Return count of changes applied."""
        n = 0
        # Prefer local recv_t_ms (when WE observed the change) so Router staleness
        # checks correctly reflect OUR book freshness from our perspective.
        ts = msg.get("recv_t_ms") or msg.get("ts")
        for c in msg.get("changes", []):
            self.apply_change(c, ts_ms=ts)
            n += 1
        return n

    def best_bid(self) -> Decimal | None:
        return max(self.bids.keys()) if self.bids else None

    def best_ask(self) -> Decimal | None:
        return min(self.asks.keys()) if self.asks else None

    def best_bid_size(self) -> Decimal:
        bb = self.best_bid()
        return self.bids.get(bb, Decimal(0)) if bb is not None else Decimal(0)

    def best_ask_size(self) -> Decimal:
        ba = self.best_ask()
        return self.asks.get(ba, Decimal(0)) if ba is not None else Decimal(0)

    def mid(self) -> Decimal | None:
        bb, ba = self.best_bid(), self.best_ask()
        if bb is None or ba is None:
            return None
        return (bb + ba) / 2

    def spread(self) -> Decimal | None:
        bb, ba = self.best_bid(), self.best_ask()
        if bb is None or ba is None:
            return None
        return ba - bb

    def spread_c(self) -> float | None:
        s = self.spread()
        return float(s * Decimal(100)) if s is not None else None

    def inside_depth_usd(self) -> tuple[Decimal, Decimal]:
        """Return (bid_inside_usd, ask_inside_usd) at top of book."""
        bb, ba = self.best_bid(), self.best_ask()
        bb_sz = self.bids.get(bb, Decimal(0)) if bb is not None else Decimal(0)
        ba_sz = self.asks.get(ba, Decimal(0)) if ba is not None else Decimal(0)
        return (
            (bb_sz * bb) if bb is not None else Decimal(0),
            (ba_sz * ba) if ba is not None else Decimal(0),
        )

    def top_n_depth_usd(self, n: int = 5) -> tuple[Decimal, Decimal]:
        """Return (top_n_bid_usd, top_n_ask_usd) summed."""
        bids_sorted = sorted(self.bids.items(), key=lambda kv: -kv[0])[:n]
        asks_sorted = sorted(self.asks.items(), key=lambda kv: kv[0])[:n]
        bid_usd = sum(p * s for p, s in bids_sorted)
        ask_usd = sum(p * s for p, s in asks_sorted)
        return bid_usd, ask_usd

    def to_dict(self) -> dict:
        bb, ba = self.best_bid(), self.best_ask()
        bb_sz, ba_sz = self.best_bid_size(), self.best_ask_size()
        return {
            "asset_id": self.asset_id,
            "market": self.market,
            "best_bid": float(bb) if bb is not None else None,
            "best_ask": float(ba) if ba is not None else None,
            "best_bid_size": float(bb_sz),
            "best_ask_size": float(ba_sz),
            "mid": float(self.mid()) if self.mid() is not None else None,
            "spread_c": self.spread_c(),
            "inside_bid_usd": float(self.inside_depth_usd()[0]),
            "inside_ask_usd": float(self.inside_depth_usd()[1]),
            "last_hash": self.last_hash,
            "last_update_ms": self.last_update_ms,
        }


@dataclass
class BookStore:
    books: dict[str, Book] = field(default_factory=dict)

    def get_or_create(self, asset_id: str, market: str = "", tick_size: str | Decimal = "0.001") -> Book:
        if asset_id not in self.books:
            self.books[asset_id] = Book(
                asset_id=asset_id, market=market, tick_size=_dec(tick_size)
            )
        return self.books[asset_id]

    def apply_ws_message(self, msg: dict) -> None:
        """Apply a normalized message from clob_ws_public.py."""
        et = msg.get("event_type")
        if et == "book":
            b = self.get_or_create(
                asset_id=msg["asset_id"],
                market=msg.get("market", ""),
                tick_size=str(msg.get("tick_size") or "0.001"),
            )
            b.apply_snapshot(msg)
        elif et == "price_change":
            for c in msg.get("changes", []):
                aid = c.get("asset_id")
                if not aid:
                    continue
                b = self.get_or_create(aid, market=msg.get("market", ""))
                b.apply_change(c, ts_ms=msg.get("ts"))
