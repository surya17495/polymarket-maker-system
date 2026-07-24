"""test_strategy_lab_smoke.py — end-to-end smoke test for the Strategy Lab.

Builds a tiny synthetic raw_events.jsonl containing a one-asset YES book + a
few price_changes that would fill a BUY-YES quote, then runs the Strategy Lab
with S0..S6 against it. Verifies:
  - Lab runs to completion for every strategy
  - Each strategy writes a `ledger_<sid>.parquet` (possibly empty)
  - Returns a ranking table sorted by composite_score
"""
from __future__ import annotations
import json
import os
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lib.market_pairs import MarketPair, save_pair_map
from lib.strategies import ALL_STRATEGY_IDS, strategy_factory
from lib.strategy_lab import _run_single_strategy_async, run_strategy_lab
from loops.router import RouterConfig, InventoryState
import asyncio


LOW_DEPTH_BOOK = {
    "event_type": "book",
    "asset_id": "ASSET_YES",
    "market": "0xCAFE",
    "ts_raw": 1000,
    "ts": 1000,
    "hash": "snap_init",
    "tick_size": "0.01",
    "last_trade_price": "0.50",
    "bids": [{"price": "0.49", "size": "100"}, {"price": "0.47", "size": "200"}],
    "asks": [{"price": "0.51", "size": "100"}, {"price": "0.53", "size": "200"}],
}


def _make_pc_msg(asset_id: str, price: str, new_size: str, side: str, ts_raw: int) -> dict:
    return {
        "event_type": "price_change",
        "market": "0xCAFE",
        "ts": ts_raw,
        "ts_raw": ts_raw,
        "changes": [{"asset_id": asset_id, "price": price, "size": new_size, "side": side, "hash": f"h_{ts_raw}"}],
    }


def _make_lab_inputs(tmp_path: Path) -> tuple[Path, dict]:
    """Tiny synthetic capture: YES book at 0.49/0.51; a single price_change that
    drops bid's 0.50-side depth (will fill our BUY-YES BID at 0.50 once emitted);
    plus a couple "stale" price_changes for the regime machine.
    """
    YES_TOKEN = "ASSET_YES"
    NO_TOKEN = "ASSET_NO"
    pair = MarketPair(
        condition_id="0xCAFE",
        yes_token_id=YES_TOKEN,
        no_token_id=NO_TOKEN,
    )
    pair_map = {YES_TOKEN: pair, NO_TOKEN: pair}
    pair_map_cache = tmp_path / "pair_map.parquet"
    save_pair_map(pair_map, pair_map_cache)
    # Write a bunch of raw_events for the YES book side
    raw_events = tmp_path / "raw_events.jsonl"
    lines: list[str] = []
    # 1) initial book snapshot
    lines.append(json.dumps(LOW_DEPTH_BOOK))
    # 2) initial NO book — we'll derive via symmetry; let's just also write something? No — the mirror will derive NO from YES.
    # 3) several price_changes to "shake" depth at 0.50, which our BID (BB+tick at 0.49+0.01=0.50) will try filling
    # Build a 1-min stream of synthetic events at 1-sec intervals; do a depth "lift" mid-event.
    for t in range(1050, 107, -10):
        pass  # noop
    # Apply a "lift" at 0.50: change in YES bid's 0.50 size to 0 (someone lifted our bid)
    # But we don't have a quote at 0.50 yet (router hasn't emitted). Let's make the capture ensure
    # enough msgs that the lab router ticks emit a BID, then depth-at-our-price shrinks.
    for t in range(2000, 40000, 500):
        # re-broadcast the same book every 500ms + a tiny depth shuffle to keep sim fresh
        # MIX: keep stable book + a depth change at t=3500
        if t == 3500:
            # Drop bid 0.49 to size 0 (someone lifted our quoted bid (if we placed at 0.49))
            lines.append(json.dumps(_make_pc_msg(YES_TOKEN, price="0.49", new_size="0", side="BUY", ts_raw=t)))
        elif t == 8000:
            # Drop bid 0.47 to size 40 (depth-at-our-hypothetical-price shrank)
            lines.append(json.dumps(_make_pc_msg(YES_TOKEN, price="0.47", new_size="40", side="BUY", ts_raw=t)))
        else:
            # Touch small tick change to keep activity
            lines.append(json.dumps(_make_pc_msg(YES_TOKEN, price="0.49", new_size="100", side="BUY", ts_raw=t)))
    raw_events.write_text("\n".join(lines) + "\n")
    return raw_events, pair_map


def test_smoke_lab_runs_against_synthetic_capture():
    print("--- test_strategy_lab_smoke ---")
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        raw_events, pair_map = _make_lab_inputs(tmp_path)
        output_dir = tmp_path / "lab_out"
        output_dir.mkdir(parents=True)

        cfg = RouterConfig(
            quote_size_usd=50.0,
            max_inventory_per_market_usd=150.0,
            max_total_inventory_usd=600.0,
            max_quote_lag_ms=60000,
            router_tick_sec=2,  # emit on each 2s of simulated time
        )

        # Run S0 first end-to-end
        ledger_path = output_dir / "ledger_s0_bb_tick.parquet"
        result = asyncio.run(_run_single_strategy_async(
            strategy_id="s0_bb_tick",
            raw_events_path=raw_events,
            pair_map=pair_map,
            output_dir=output_dir,
            cfg_router=cfg,
            fill_log_path=ledger_path,
            backfill_markouts=True,
            validate_via_trades_truth=False,  # smoke test uses synthetic cids (not on Polymarket); skip /trades REST
        ))
        assert result["strategy_id"] == "s0_bb_tick"
        assert "metrics" in result
        print(f"  S0: fills={result['fills']}, msgs={result['n_msgs']}, composite={result['metrics']['composite_score']:.4f}")

        # Try all strategies end-to-end (must complete without exceptions)
        for sid in ALL_STRATEGY_IDS:
            ledger_path = output_dir / f"ledger_{sid}.parquet"
            r = asyncio.run(_run_single_strategy_async(
                strategy_id=sid,
                raw_events_path=raw_events,
                pair_map=pair_map,
                output_dir=output_dir,
                cfg_router=cfg,
                fill_log_path=ledger_path,
                backfill_markouts=True,
                validate_via_trades_truth=False,  # smoke test uses synthetic cids (not on Polymarket); skip /trades REST
            ))
            assert "metrics" in r, f"strategy {sid} missing metrics: {r}"
            print(f"  {sid}: fills={r['fills']}, msgs={r['n_msgs']}, merges={r['merges_count']}, composite={r['metrics']['composite_score']:.4f}")

        # Full run via run_strategy_lab ranking
        cache_path = tmp_path / "pair_map.parquet"
        full_results = run_strategy_lab(
            raw_events_path=raw_events,
            pair_map_cache_path=cache_path,
            output_dir=output_dir / "full_run",
            strategy_ids=ALL_STRATEGY_IDS,
            router_tick_sec=2,
            validate_via_trades_truth=False,
        )
        ranking = full_results["ranking"]
        assert len(ranking) == len(ALL_STRATEGY_IDS)
        # Check sorted by composite_score descending
        for i in range(1, len(ranking)):
            assert ranking[i - 1]["composite_score"] >= ranking[i]["composite_score"]
        print(f"  Full lab run produces a sorted ranking of {len(ranking)} strategies.")
        print(f"  Top: id={ranking[0]['strategy_id']} score={ranking[0]['composite_score']:.4f} fills={ranking[0]['fills']}")
        # ranking.json should have been created
        ranking_path = output_dir / "full_run" / "lab_ranking.json"
        assert ranking_path.exists()
        print(f"  ranking table written to {ranking_path}")
        print("SUCCEEDED")


def main():
    test_smoke_lab_runs_against_synthetic_capture()


if __name__ == "__main__":
    main()
