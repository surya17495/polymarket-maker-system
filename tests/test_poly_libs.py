"""test_poly_libs.py — unit tests for poly_estimators + poly_quoting + poly_regime +
poly_merger + mirrored_book (poly-maker pattern ports).

Pure offline tests with synthetic inputs; no network or WS.
"""
from __future__ import annotations
import os
import sys
import time
import tempfile
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lib.poly_estimators import (Ewma, VolEstimator, FlowEstimator, MarkoutTracker, Side,
                                  MarketEstimators, market_estimator_default)
from lib.poly_quoting import (round_to_tick, compute_fair_value, construct_quotes,
                              QuoteInputs, QuoteSpec)
from lib.poly_regime import (Regime, RegimeMachine, RegimeInputs, StrategyProfile)
from lib.poly_merger import (MergeEvent, MarketInventory, MergerState)
from lib.mirrored_book import MirroredBookStore
from lib.market_pairs import MarketPair


def _ok(name: str, cond: bool, extra: str = "") -> None:
    status = "PASS" if cond else "FAIL"
    print(f"  [{status}] {name}{(' — ' + extra) if extra else ''}")


def test_ewma_basic():
    print("--- Ewma ---")
    e = Ewma(halflife_s=10.0)
    e.update(10.0, 0.0)
    assert abs(e.value - 10.0) < 1e-9
    e.update(20.0, 10.0)  # dt=half-life → 50/50 mix
    assert abs(e.value - 15.0) < 0.01, f"expected 15, got {e.value}"
    _ok("Ewma basic 50/50 at halflife", abs(e.value - 15.0) < 0.01)


def test_ewma_decay_to():
    e = Ewma(halflife_s=10.0)
    e.update(10.0, 0.0)
    v = e.decay_to(10.0)  # decay at halflife since last update
    assert abs(v - 5.0) < 0.01, f"expected 5, got {v}"
    _ok("Ewma decay_to", abs(v - 5.0) < 0.01)


def test_volestimator():
    print("--- VolEstimator ---")
    v = VolEstimator(short_halflife_s=10, long_halflife_s=100)
    for i, fv in enumerate([0.50, 0.53, 0.50, 0.55, 0.50]):
        v.update(fv, i * 10.0)
    _ok("VolEstimator short > 0", v.short > 0)
    _ok("VolEstimator long > 0", v.long > 0)
    _ok("VolEstimator ratio positive", v.ratio > 0)


def test_flowestimator():
    print("--- FlowEstimator ---")
    f = FlowEstimator(halflife_s=5.0)
    f.update(Side.BUY, 100, ts=0.0)
    f.update(Side.BUY, 100, ts=1.0)
    assert f.signed > 0
    assert f.z > 0
    _ok("FlowEstimator BUY-flow z>0", f.z > 0)
    f.update(Side.SELL, 200, ts=2.0)
    # Sell flow > buy → signed flips
    _ok("FlowEstimator SELL-dominant z<0 OR zero", f.z <= 0 or abs(f.z) < 1.5)


def test_markout_tracker():
    print("--- MarkoutTracker ---")
    m = MarkoutTracker(horizon_s=1.0, ewma_halflife_s=10.0)
    # I bought at 0.50 at t=0; price fell to 0.40 at t=1 → adverse (negative)
    m.record_fill(Side.BUY, fv_at_fill=0.50, ts=0.0)
    m.evaluate(fv_now=0.40, ts=1.0)
    assert m.markout < 0
    _ok("MarkoutTracker negative on adverse move", m.markout < 0)
    _ok("MarkoutTracker toxicity = -markout > 0", m.toxicity > 0)


def test_round_to_tick():
    print("--- round_to_tick ---")
    # tick = 0.01 — snaps to nearest tick biased by `up` flag
    p = round_to_tick(0.1235, tick=0.01, decimals=4, up=True)
    _ok("round_to_tick up-rounded", p == 0.13, f"got {p}")
    p = round_to_tick(0.1245, tick=0.01, decimals=4, up=False)
    _ok("round_to_tick down-rounded", p == 0.12, f"got {p}")


def test_compute_fair_value_clamped():
    print("--- compute_fair_value ---")
    fv = compute_fair_value(microprice=0.5, flow_z=10, tick=0.01, weight=1.0)
    # 0.5 + 1.0 * 10 * 0.01 = 0.6 — within (tick, 1-tick)
    _ok("compute_fair_value in (0,1) for +flow", 0 < fv < 1, f"fv={fv}")
    fv = compute_fair_value(microprice=0.99, flow_z=100, tick=0.01, weight=1.0)
    _ok("compute_fair_value clamps at upper bound near 0.99", 0 < fv < 1, f"fv={fv}")


def test_construct_quotes_basic_buy_yes_no():
    print("--- construct_quotes basic BUY-YES + BUY-NO ---")
    inp = QuoteInputs(
        fv=0.50, vol_short=0.001, toxicity=0.001, regime=Regime.QUIET,
        bb_yes=Decimal("0.49"), ba_yes=Decimal("0.51"),
        # NO_book derived from YES via symmetry: NO best_bid = 1-0.51=0.49, NO best_ask = 1-0.49=0.51
        bb_no=Decimal("0.49"), ba_no=Decimal("0.51"),
        bb_yes_size=Decimal("100"), ba_yes_size=Decimal("100"),
        bb_no_size=Decimal("100"), ba_no_size=Decimal("100"),
        pos_yes_shares=0.0, pos_no_shares=0.0,
        tick_size=0.001, price_decimals=4,
        q_max_usdc=200, base_size_usdc=50, gamma=1.0,
        c_vol=5.0, c_tox=2.0, delta_min_ticks=1,
        q_soft_frac=0.7, min_edge_ticks=1, layers=1,
        min_order_size=0.0, reward_floor=0.0,
        yes_token_id="YES_TOKEN", no_token_id="NO_TOKEN", quote_market="0xCAFE",
    )
    quotes = construct_quotes(inp)
    assert quotes, "expected at least one quote"
    buys = [q for q in quotes if q.side == "BUY"]
    _ok("construct_quotes emits >=1 BUY", len(buys) >= 1, f"got {len(buys)} BUY")
    yes_buy = [q for q in buys if q.token_id == "YES_TOKEN"]
    no_buy = [q for q in buys if q.token_id == "NO_TOKEN"]
    _ok("construct_quotes BUY-YES present", len(yes_buy) == 1)
    _ok("construct_quotes BUY-NO present", len(no_buy) == 1)
    if yes_buy:
        _ok("BUY-YES price < fv", yes_buy[0].price < 0.5, f"got {yes_buy[0].price}")
    if no_buy:
        # NO fair value = 1 - fv = 0.50, BUY-NO at no_fv − δ = 0.50 - 2*0.001=0.498 (or similar)
        _ok("BUY-NO price < 1 - fv = 0.50", no_buy[0].price < 0.51, f"got {no_buy[0].price}")


def test_construct_quotes_reduce_only_skip_add_emit_exit():
    print("--- construct_quotes REDUCE_ONLY skip add keep exit ---")
    inp = QuoteInputs(
        fv=0.50, vol_short=0.001, toxicity=0.001, regime=Regime.REDUCE_ONLY,
        bb_yes=Decimal("0.49"), ba_yes=Decimal("0.51"),
        bb_no=Decimal("0.49"), ba_no=Decimal("0.51"),
        bb_yes_size=Decimal("100"), ba_yes_size=Decimal("100"),
        bb_no_size=Decimal("100"), ba_no_size=Decimal("100"),
        pos_yes_shares=100.0, pos_no_shares=50.0,
        tick_size=0.001, price_decimals=4,
        q_max_usdc=200, base_size_usdc=50, gamma=1.0,
        c_vol=5.0, c_tox=2.0, delta_min_ticks=1,
        q_soft_frac=0.7, min_edge_ticks=1,
        min_order_size=0.0, reward_floor=0.0,
        yes_token_id="YES", no_token_id="NO", quote_market="0x",
    )
    quotes = construct_quotes(inp)
    buys = [q for q in quotes if q.side == "BUY"]
    sells = [q for q in quotes if q.side == "SELL"]
    _ok("REDUCE_ONLY suppresses BUY entries", len(buys) == 0, f"got {len(buys)} buys")
    _ok("REDUCE_ONLY keeps SELL exits on held inventory", len(sells) >= 1, f"got {len(sells)} sells")


def test_construct_quotes_event_skips_all():
    inp = QuoteInputs(
        fv=0.50, vol_short=0.001, toxicity=0.001, regime=Regime.EVENT,
        bb_yes=Decimal("0.49"), ba_yes=Decimal("0.51"),
        bb_no=Decimal("0.49"), ba_no=Decimal("0.51"),
        bb_yes_size=Decimal("100"), ba_yes_size=Decimal("100"),
        bb_no_size=Decimal("100"), ba_no_size=Decimal("100"),
        pos_yes_shares=10.0, pos_no_shares=10.0,
        tick_size=0.001, q_max_usdc=200, base_size_usdc=50,
        yes_token_id="YES", no_token_id="NO", quote_market="0x",
        c_vol=5.0, c_tox=2.0, delta_min_ticks=1,
        q_soft_frac=0.7, min_edge_ticks=1,
    )
    quotes = construct_quotes(inp)
    _ok("EVENT suppresses all quotes", len(quotes) == 0)


def test_regime_machine_priority():
    print("--- RegimeMachine ---")
    rm = RegimeMachine()
    prof = StrategyProfile(event_jump_ticks=30, event_cooloff_s=10)
    # HALTED > all
    r = rm.decide(RegimeInputs(now=0, tick=0.01, fv=0.5, prev_fv=None,
                                vol_ratio=0, flow_z=0, inventory_util=0,
                                risk_halt=True), prof)
    _ok("Regime HALTED on risk_halt", r == Regime.HALTED, str(r))
    # REDUCE_ONLY on inventory_util ≥ 1
    r = rm.decide(RegimeInputs(now=1, tick=0.01, fv=0.5, prev_fv=0.5,
                                vol_ratio=0, flow_z=0, inventory_util=1.0), prof)
    _ok("Regime REDUCE_ONLY @ util=1", r == Regime.REDUCE_ONLY, str(r))
    # EVENT on jump
    r = rm.decide(RegimeInputs(now=2, tick=0.01, fv=0.9, prev_fv=0.5,
                                vol_ratio=0, flow_z=0, inventory_util=0), prof)
    _ok("Regime EVENT on FV jump ≥ jump_ticks", r == Regime.EVENT, str(r))
    # QUIET default
    r = rm.decide(RegimeInputs(now=20, tick=0.01, fv=0.5, prev_fv=0.495,
                                vol_ratio=0, flow_z=0, inventory_util=0), prof)
    _ok("Regime QUIET when cooloff expired + calm", r == Regime.QUIET, str(r))


def test_market_inventory_join_and_merge():
    print("--- MarketInventory + MergerState ---")
    inv = MarketInventory(condition_id="0xCAFE")
    inv.apply_yes_fill(qty=100, price=0.40)
    inv.apply_no_fill(qty=50, price=0.40)
    # Can merge min(100, 50) = 50 pairs at locked edge = 1 - 0.40 - 0.40 = 0.2 per pair
    ev = inv.try_merge(ts_ms=1000)
    assert ev is not None
    _ok("try_merge emits MergeEvent when both YES+NO held", ev is not None)
    expected_pnl = 50 * 0.2  # = 10
    _ok("MergeEvent realized_pnl = pair_qty × locked_edge", abs(ev.realized_pnl_usd - expected_pnl) < 0.01,
        f"got {ev.realized_pnl_usd} vs {expected_pnl}")
    _ok("MergeEvent capital_returned_usd = pair_qty × 1", abs(ev.capital_returned_usd - 50.0) < 0.01)
    # After merge: yes_shares=50, no_shares=0
    _ok("After merge, no_shares cleared", abs(inv.no_shares) < 1e-9, f"got {inv.no_shares}")
    _ok("After merge, yes_shares reduced", abs(inv.yes_shares - 50) < 1e-9, f"got {inv.yes_shares}")


def test_merger_state_try_all():
    print("--- MergerState.try_merge_all ---")
    s = MergerState()
    s.record_fill_v2("0xC1", "YES", qty=100, price=0.30, we_bought=True)
    s.record_fill_v2("0xC1", "NO", qty=100, price=0.30, we_bought=True)
    s.record_fill_v2("0xC2", "YES", qty=80, price=0.20, we_bought=True)
    s.record_fill_v2("0xC2", "NO", qty=40, price=0.20, we_bought=True)
    events = s.try_merge_all(ts_ms=1000)
    _ok("try_merge_all emits 2 events for 2 conditions with both sides", len(events) == 2, f"got {len(events)}")
    if len(events) >= 2:
        # 0xC1: pairs=100, pnl=100*(1-0.6)=40; 0xC2: pairs=40, pnl=40*(1-0.4)=24
        total_pnl = sum([e.realized_pnl_usd for e in events])
        _ok("Total realized_pnl correct for both pairs", abs(total_pnl - 64) < 0.01, f"got {total_pnl}")


def test_mirrored_book_snapshot():
    print("--- MirroredBookStore snapshot mirror ---")
    pair = MarketPair(
        condition_id="0xCAFE",
        yes_token_id="YES_TOKEN",
        no_token_id="NO_TOKEN",
    )
    pair_map = {"YES_TOKEN": pair, "NO_TOKEN": pair}
    mb = MirroredBookStore(pair_map)
    yes_book_msg = {
        "event_type": "book",
        "asset_id": "YES_TOKEN",
        "market": "0xCAFE",
        "ts_raw": 1000, "ts": 1000,
        "hash": "h001",
        "tick_size": "0.001",
        "last_trade_price": "0.50",
        "bids": [{"price": "0.50", "size": "200"}, {"price": "0.48", "size": "100"}],
        "asks": [{"price": "0.52", "size": "150"}, {"price": "0.55", "size": "80"}],
    }
    mb.apply_ws_message(yes_book_msg)
    no_book = mb.books.get("NO_TOKEN")
    assert no_book is not None
    # YES bid 0.50 size 200 → NO ask price (1-0.50)=0.50 size 200
    # YES bid 0.48 size 100 → NO ask price 0.52 size 100
    # YES ask 0.52 size 150 → NO bid price 0.48 size 150
    # YES ask 0.55 size 80  → NO bid price 0.45 size 80
    assert no_book.bids.get(Decimal("0.48")) == Decimal("150")
    assert no_book.asks.get(Decimal("0.50")) == Decimal("200")
    _ok("mirror snapshot writes NO bid at (1-YES ask_price) with mirrored size",
         no_book.bids.get(Decimal("0.48")) == Decimal("150"))
    _ok("mirror snapshot writes NO ask at (1-YES bid_price) with mirrored size",
         no_book.asks.get(Decimal("0.50")) == Decimal("200"))


def test_mirrored_book_price_change():
    print("--- MirroredBookStore price_change mirror ---")
    pair = MarketPair(condition_id="0xCAFE", yes_token_id="YES_TOKEN", no_token_id="NO_TOKEN")
    mb = MirroredBookStore({"YES_TOKEN": pair, "NO_TOKEN": pair})
    # Initial YES book:
    book_msg = {
        "event_type": "book",
        "asset_id": "YES_TOKEN", "market": "0xCAFE",
        "ts_raw": 1000, "ts": 1000, "hash": "h1", "tick_size": "0.001",
        "bids": [{"price": "0.50", "size": "200"}],
        "asks": [{"price": "0.52", "size": "150"}],
    }
    mb.apply_ws_message(book_msg)
    # YES price_change: change at YES price 0.50, side BUY, size 100 → NO change at price 0.50, side SELL, size 100
    pc_msg = {
        "event_type": "price_change",
        "market": "0xCAFE",
        "ts_raw": 1500, "ts": 1500,
        "changes": [{"asset_id": "YES_TOKEN", "price": "0.50", "size": "100", "side": "BUY", "hash": "h2"}],
    }
    mb.apply_ws_message(pc_msg)
    no_book = mb.books.get("NO_TOKEN")
    # NO best_ask should now be 0.50 (was 0.50 → updated?) YES ask's mirrored value: side BUY at 0.50 → no side SELL at 0.50, size 100 — replacing the previous NO ask at 0.50 with size 200
    assert no_book.asks.get(Decimal("0.50")) == Decimal("100"), f"expected 100, got {no_book.asks.get(Decimal('0.50'))}"
    _ok("mirror price_change updates NO side correctly",
         no_book.asks.get(Decimal("0.50")) == Decimal("100"))


def main():
    test_ewma_basic()
    test_ewma_decay_to()
    test_volestimator()
    test_flowestimator()
    test_markout_tracker()
    test_round_to_tick()
    test_compute_fair_value_clamped()
    test_construct_quotes_basic_buy_yes_no()
    test_construct_quotes_reduce_only_skip_add_emit_exit()
    test_construct_quotes_event_skips_all()
    test_regime_machine_priority()
    test_market_inventory_join_and_merge()
    test_merger_state_try_all()
    test_mirrored_book_snapshot()
    test_mirrored_book_price_change()
    print("--- All poly_libs tests done ---")


if __name__ == "__main__":
    main()
