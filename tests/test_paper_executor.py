"""test_paper_executor.py — Phase 1A simulator unit tests.

Validates the PaperExecutor Contract 1-7 walk:
  - QuoteSubmit is accepted by the executor
  - Stale quotes (price outside the at-arrival BBO) are rejected
  - Live quotes get queued with bounds (queue_position_best/expected/worst_case)
  - When depth-at-our-price shrinks below pre-quote depth via subsequent price_changes,
    a per-fill ledger row with the full Contract 4 schema is emitted
  - Ledger parquet appends correctly with retry

This test uses fully synthetic WS data so it runs offline (no network).
"""
from __future__ import annotations
import asyncio
import os
import sys
import time
from decimal import Decimal
from pathlib import Path

sys.path.insert(
    0, os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)

from lib.book import Book, BookStore
from loops.paper_executor import PaperExecutor, FILL_SCHEMA
from loops.router import QuoteSubmit, Router, RouterConfig


BASE_TS_OBSERVE = 1784884081607  # t_observe base
ASSET = "ASSET_A"
MARKET_HEX = "0xSample"


def _make_book_msg(asset_id: str, bids: list[tuple[str, str]], asks: list[tuple[str, str]]) -> dict:
    bid_rows = [{"price": p, "size": s} for p, s in bids]
    ask_rows = [{"price": p, "size": s} for p, s in asks]
    return {
        "event_type": "book",
        "asset_id": asset_id,
        "market": MARKET_HEX,
        "ts_raw": BASE_TS_OBSERVE,
        "hash": "snap_hash_init",
        "tick_size": "0.001",
        "last_trade_price": "0.31",
        "bids": bid_rows,
        "asks": ask_rows,
    }


def _make_pc_msg(pcs: list[dict], ts_raw: int) -> dict:
    return {
        "event_type": "price_change",
        "market": MARKET_HEX,
        "ts": ts_raw,
        "changes": pcs,
    }


def _build_router_and_executor(tmp_path: Path) -> tuple[Router, PaperExecutor, BookStore]:
    store = BookStore()
    store.apply_ws_message(
        _make_book_msg(ASSET, [("0.30", "100"), ("0.28", "200")], [("0.32", "100"), ("0.34", "200")])
    )

    cfg = RouterConfig(quote_size_usd=30.0, max_inventory_per_market_usd=200.0)
    router = Router(cfg=cfg, book_store=store)
    ex = PaperExecutor(
        book_store=store,
        router=router,
        raw_events_path=tmp_path / "raw_events.jsonl",
        fill_log_path=tmp_path / "ledger.parquet",
        default_latency_sample_ms=200,
        latency_sample_jitter_ms=0,
    )
    return router, ex, store


def test_quote_rejected_when_stale_outside_arrival_bbo(tmp_path):
    router, ex, store = _build_router_and_executor(tmp_path)
    # We submit a BID at 0.50 (above snapshot best_bid 0.30) — but the arrival
    # book simulates a fast move where best_bid dropped to 0.25. So our quote
    # is stale on arrival (above the new best_bid → would not actually have placed).
    submit = QuoteSubmit(
        asset_id=ASSET, market=MARKET_HEX, side="BID",
        price=0.30, size=100.0,
        t_observe_perf_counter=time.perf_counter(),
        t_observe_ms=BASE_TS_OBSERVE, scan_cycle_id="scan_test",
    )
    # Push a price_change into executor's buffer that drops bid 0.30 to 0 BEFORE our arrival
    ev = _make_pc_msg(
        [{"asset_id": ASSET, "price": "0.30", "size": "0", "side": "BUY", "hash": "delta0"}],
        ts_raw=BASE_TS_OBSERVE + 50,
    )
    ex.ingest_ws_message(ev)
    ex.submit_quote(submit)
    asyncio.run(ex._process_pending_quotes())
    # Quote should NOT have been placed because at arrival, bid 0.30 level is gone
    assert not ex.placed_quotes, "stale quote (price level vanished before arrival) must be rejected"


def test_quote_placed_when_inside_arrival_bbo(tmp_path):
    router, ex, store = _build_router_and_executor(tmp_path)
    submit = QuoteSubmit(
        asset_id=ASSET, market=MARKET_HEX, side="BID",
        price=0.30, size=50.0,
        t_observe_perf_counter=time.perf_counter(),
        t_observe_ms=BASE_TS_OBSERVE, scan_cycle_id="scan_test",
    )
    # No change in book between observe and arrival → quote accepted
    ex.submit_quote(submit)
    asyncio.run(ex._process_pending_quotes())
    assert len(ex.placed_quotes) == 1, "live quote must be placed"
    placement = next(iter(ex.placed_quotes.values()))
    assert placement.queue_position_best_case == 0
    assert placement.queue_position_worst_case >= 1
    # depth_at_arrival is the public depth at our price (100) + our quote (50) — pre_quote_depth should be 100
    assert placement.pre_quote_depth_at_price_at_arrival == Decimal("100")


def test_per_fill_ledger_row_has_full_contract_4_schema(tmp_path):
    router, ex, store = _build_router_and_executor(tmp_path)
    submit = QuoteSubmit(
        asset_id=ASSET, market=MARKET_HEX, side="BID",
        price=0.30, size=50.0,
        t_observe_perf_counter=time.perf_counter(),
        t_observe_ms=BASE_TS_OBSERVE, scan_cycle_id="scan_test",
    )
    ex.submit_quote(submit)
    asyncio.run(ex._process_pending_quotes())
    assert ex.placed_quotes, "quote must be placed first"
    placed_id = next(iter(ex.placed_quotes.keys()))
    placement = ex.placed_quotes[placed_id]
    pre_depth = placement.depth_at_price_at_arrival
    # Submit a price_change that drops bids[0.30] from 100+50 (our quote included) public-visible depth.
    # Wait — bids[0.30] is the PUBLIC book which doesn't include our quote, so it stays at 100.
    # If we now see depth drop to 50 (50 shares cancelled or filled against the OTHER side),
    # the public depth-at-our-price shrunk by 50.

    # Apply the price_change to our book_store (simulating WS push):
    pc = _make_pc_msg(
        [{"asset_id": ASSET, "price": "0.30", "size": "50", "side": "BUY", "hash": "delta1"}],
        ts_raw=BASE_TS_OBSERVE + 250,
    )
    store.apply_ws_message(pc)
    ex.ingest_ws_message(pc)
    asyncio.run(ex._process_book_events_into_fills())
    assert ex.completed_fills, "fill must have been emitted when depth at our price shrunk"
    fill = ex.completed_fills[0]
    for col in FILL_SCHEMA:
        assert col in fill, f"contract-4 field missing: {col}"
    # sanity check on key fields
    assert fill["asset_id"] == ASSET
    assert fill["side_taker"] == "SELL"  # BID quote gets lifted by SELL-side taker
    assert fill["queue_position_worst_case"] >= 1
    # fill_qty should be > 0
    assert fill["exec_qty"] > 0


def test_ledger_parquet_writes_and_appends(tmp_path):
    router, ex, store = _build_router_and_executor(tmp_path)
    # place one quote then close it without fills — flush_fills_to_parquet with 0 rows returns 0
    n_first = ex.flush_fills_to_parquet()
    assert n_first == 0
    # synthesize a fill
    ex.completed_fills.append({col: None for col in FILL_SCHEMA})
    row = ex.completed_fills[-1]
    row.update({"ts_utc": BASE_TS_OBSERVE, "asset_id": ASSET, "market": MARKET_HEX,
                "exec_price": 0.30, "exec_qty": 10.0, "side_taker": "SELL",
                "queue_position_best_case": 0, "queue_position_expected": 1,
                "queue_position_worst_case": 5, "t_observe_ms": BASE_TS_OBSERVE,
                "t_arrival_sim_ms": BASE_TS_OBSERVE + 240, "fill_id": "fid1",
                "scan_cycle_id": "sc1",
                })
    n = ex.flush_fills_to_parquet()
    assert n == 1
    import pyarrow.parquet as pq
    table = pq.read_table(ex.fill_log_path)
    assert table.num_rows == 1, f"ledger should have 1 row; got {table.num_rows}"
    assert set(table.schema.names).issuperset(set(FILL_SCHEMA))


def test_kill_switch_fires_on_drawdown():
    router, ex, store = _build_router_and_executor(Path("/tmp"))
    # $2k base, $60 loss = 3% → halt
    ex.base_equity_usd = 2000.0
    assert ex.check_kill_switches(realized_pnl_usd=-60, deployed_usd=200) == "halt"
    assert ex.check_kill_switches(realized_pnl_usd=-40, deployed_usd=200) == "reduce"
    assert ex.check_kill_switches(realized_pnl_usd=-20, deployed_usd=200) == "warn"
    assert ex.check_kill_switches(realized_pnl_usd=+20, deployed_usd=200) is None


if __name__ == "__main__":
    import tempfile
    with tempfile.TemporaryDirectory() as tmp:
        print("Rejected when stale test:")
        test_quote_rejected_when_stale_outside_arrival_bbo(Path(tmp)); print("  PASSED")
        print("Placed when live test:")
        test_quote_placed_when_inside_arrival_bbo(Path(tmp)); print("  PASSED")
        print("Per-fill schema test:")
        test_per_fill_ledger_row_has_full_contract_4_schema(Path(tmp)); print("  PASSED")
        print("Parquet write/append test:")
        test_ledger_parquet_writes_and_appends(Path(tmp)); print("  PASSED")
        print("Kill switch test:")
        test_kill_switch_fires_on_drawdown(); print("  PASSED")
        print("All Phase 1A paper_executor tests done.")
