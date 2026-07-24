"""mirrored_book.py — MirroredBookStore auto-derives NO books from YES books via
the symmetric YES + NO = $1 binary market invariant.

For each YES book event received on `yes_token`, this store ALSO writes a derived
NO book under `no_token` such that:

  NO best_bid = 1 − YES best_ask            (size mirrored)
  NO best_ask = 1 − YES best_bid            (size mirrored)
  NO price_change side SELL ← YES price_change side BUY at NO_price = 1 − p_yes
  NO price_change side BUY  ← YES price_change side SELL at NO_price = 1 − p_yes

The derivation is exact for Polymarket's binary YES+NO market structures; for the
Phase 1A offline Strategy Lab, this grants us TWO-sided market books even when the
live capture subscribed only to the YES token, so S1..S6 (poly-quoting and merge
strategies) can quote and fill on both sides.

Pair map: dict[(asset_id)->MarketPair]. Constructed by lib/market_pairs.py.
"""
from __future__ import annotations
from decimal import Decimal
from typing import Iterable

from lib.book import Book, BookStore


def _dec(v) -> Decimal:
    if v is None:
        return Decimal(0)
    if isinstance(v, Decimal):
        return v
    try:
        return Decimal(str(v))
    except Exception:
        return Decimal(0)


def _mirror_levels_to_levels(
    levels: Iterable[dict],
    *,
    from_side: str,
) -> dict[Decimal, Decimal]:
    """Convert a YES side's price-levels to NO side levels with mirrored price+size.
    
    YES BUY at YES p YES s   ↔ NO SELL at NO p = (1 − YES p) YES s  (size n_bids_zones YES == size n_asks_zones NO)
    YES SELL at YES p YES s  ↔ NO BUY  at NO p = (1 − YES p) YES s
    """
    out: dict[Decimal, Decimal] = {}
    for entry in (levels or []):
        p = _dec(entry.get("price"))
        s = _dec(entry.get("size"))
        if p <= 0 or s <= 0:
            continue
        # Mirror price: NO side = 1 - YES price, clamped to >0
        opp_price = Decimal("1") - p
        if opp_price <= 0:
            continue
        out[opp_price] = s
    return out


def _mirror_book_snapshot_to_no(yes_book_msg: dict, no_token_id: str) -> dict:
    """Translate a `book` snapshot message keyed by yes_token_id into the no-token's
    equivalent snapshot.
    """
    yes_bids = yes_book_msg.get("bids") or []
    yes_asks = yes_book_msg.get("asks") or []
    # NO asks come from YES bids; NO bids come from YES asks.
    # YES bid at price p size s   ↔   NO ask at (1-p) size s  (someone selling NO equivalent to buying YES)
    # YES ask at price p size s   ↔   NO bid at (1-p) size s
    yd_pair = [(str(_dec(e.get("price"))), str(_dec(e.get("size")))) for e in yes_bids]
    ya_pair = [(str(_dec(e.get("price"))), str(_dec(e.get("size")))) for e in yes_asks]
    no_asks = []
    for p_str, s_str in yd_pair:
        p_yes = _dec(p_str); s = _dec(s_str)
        if p_yes <= 0 or s <= 0:
            continue
        opp_p = Decimal("1") - p_yes
        if opp_p <= 0:
            continue
        no_asks.append({"price": str(opp_p), "size": str(s)})
    no_bids = []
    for p_str, s_str in ya_pair:
        p_yes = _dec(p_str); s = _dec(s_str)
        if p_yes <= 0 or s <= 0:
            continue
        opp_p = Decimal("1") - p_yes
        if opp_p <= 0:
            continue
        no_bids.append({"price": str(opp_p), "size": str(s)})
    return {
        "event_type": "book",
        "asset_id": no_token_id,
        "market": yes_book_msg.get("market", ""),
        "ts_raw": yes_book_msg.get("ts_raw") or yes_book_msg.get("ts"),
        "ts": yes_book_msg.get("ts_raw") or yes_book_msg.get("ts"),
        "hash": no_token_id[:12] + "_mirror",
        "tick_size": yes_book_msg.get("tick_size", "0.001"),
        "last_trade_price": yes_book_msg.get("last_trade_price", "0"),
        "bids": no_bids,
        "asks": no_asks,
    }


def _mirror_change_to_no(change: dict, no_token_id: str) -> dict:
    """Translate a YES-side price_change to NO-side: invert price + side.
    
    side="BUY" (YES bid) at p_yes ↔ NO side="SELL" at p_no = 1 − p_yes.
    side="SELL" (YES ask) at p_yes ↔ NO side="BUY" at p_no = 1 − p_yes.
    """
    p_yes = _dec(change.get("price"))
    size = _dec(change.get("size"))
    s_yes = change.get("side") or "BUY"
    out = {
        "asset_id": no_token_id,
        "price": str(Decimal("1") - p_yes) if p_yes > 0 else str(p_yes),
        "size": str(size),
        "side": "SELL" if s_yes == "BUY" else "BUY",
    }
    if "hash" in change:
        out["hash"] = change["hash"] + "_mirror"
    if change.get("best_bid") is not None:
        out["best_bid"] = str(Decimal("1") - _dec(change.get("best_ask") or change.get("best_bid")))
    if change.get("best_ask") is not None:
        out["best_ask"] = str(Decimal("1") - _dec(change.get("best_bid") or change.get("best_ask")))
    return out


def _mirror_pc_to_no(pc_msg: dict, no_token_id: str) -> dict:
    """Mirror a full price_change message (with `changes` array) onto a no_token."""
    out_changes = []
    for c in (pc_msg.get("changes") or []):
        out_changes.append(_mirror_change_to_no(c, no_token_id))
    return {
        "event_type": "price_change",
        "market": pc_msg.get("market", ""),
        "ts": pc_msg.get("ts"),
        "ts_raw": pc_msg.get("ts_raw"),
        "changes": out_changes,
    }


class MirroredBookStore:
    """BookStore wrapper that auto-derives NO books for each YES.book event received.
    
    pair_map: dict[asset_id -> MarketPair]; built by lib/market_pairs.py.
    """
    def __init__(self, pair_map: dict):
        self._inner = BookStore()
        self._pair_map = pair_map
        self._pair_by_yes = {}
        self._pair_by_no = {}
        for asset_id, pair in pair_map.items():
            if pair is None or not getattr(pair, "yes_token_id", ""):
                continue
            self._pair_by_yes[pair.yes_token_id] = pair
            self._pair_by_no[pair.no_token_id] = pair

    @property
    def books(self):
        return self._inner.books

    def get_pair(self, asset_id: str):
        return self._pair_map.get(asset_id)

    def get_inner(self) -> BookStore:
        return self._inner

    def apply_ws_message(self, msg: dict) -> None:
        """Apply message to YES book; auto-derive NO book event if applicable."""
        self._inner.apply_ws_message(msg)
        et = msg.get("event_type")
        if et == "book":
            yes_token = msg.get("asset_id")
            pair = self._pair_by_yes.get(yes_token)
            if pair:
                mirrored = _mirror_book_snapshot_to_no(msg, pair.no_token_id)
                self._inner.apply_ws_message(mirrored)
        elif et == "price_change":
            for c in (msg.get("changes") or []):
                yes_token = c.get("asset_id")
                pair = self._pair_by_yes.get(yes_token)
                if pair:
                    # Mirror each YES change onto NO change
                    no_change = _mirror_change_to_no(c, pair.no_token_id)
                    # Construct a synthetic pc message containing one change
                    mirrored_pc = {
                        "event_type": "price_change",
                        "market": msg.get("market", ""),
                        "ts": msg.get("ts"),
                        "ts_raw": msg.get("ts_raw"),
                        "changes": [no_change],
                    }
                    self._inner.apply_ws_message(mirrored_pc)


__all__ = ["MirroredBookStore"]
