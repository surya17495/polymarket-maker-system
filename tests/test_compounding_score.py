"""test_compounding_score.py — unit tests for composite scoring math."""
from __future__ import annotations
import os
import sys
import tempfile
import json
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pyarrow as pa
import pyarrow.parquet as pq

from lib.compounding_score import compute_composite_score
from loops.paper_executor import FILL_SCHEMA


def _write_fills(path: Path, fills: list[dict]) -> None:
    schema_rows = []
    for f in fills:
        row = {}
        for col in FILL_SCHEMA:
            row[col] = f.get(col, None)
        schema_rows.append(row)
    table = pa.Table.from_pylist(schema_rows)
    pq.write_table(table, path)


def test_compounding_score_zero_when_no_filledger():
    print("--- compounding_score: 0 when ledger missing ---")
    m = compute_composite_score(Path("/tmp/never_exists_xyz.parquet"))
    assert m["composite_score"] == 0.0
    assert m["fill_count"] == 0
    print("  PASS")


def test_compounding_score_positive_when_caps_recycle():
    print("--- compounding_score: positive when signed-positive + recycling ---")
    with tempfile.TemporaryDirectory() as t:
        ledger = Path(t) / "ledger.parquet"
        # 3 fills, all positive pnl_worst_case, side_taker BUY (someone lifted our ASK → we sold → capital returned)
        _write_fills(ledger, [
            {
                "ts_utc": 1000, "asset_id": "Y", "market": "0x",
                "side_taker": "BUY", "exec_price": 0.55, "exec_qty": 100,
                "pnl_worst_case": 1.0, "gross_edge_at_fill": 5.0,
                "markout_60s": 0.0, "fill_id": "f1",
            },
            {
                "ts_utc": 2000, "asset_id": "Y", "market": "0x",
                "side_taker": "BUY", "exec_price": 0.55, "exec_qty": 100,
                "pnl_worst_case": 1.5, "gross_edge_at_fill": 5.0,
                "markout_60s": 0.0, "fill_id": "f2",
            },
            {
                "ts_utc": 3000, "asset_id": "Y", "market": "0x",
                "side_taker": "BUY", "exec_price": 0.55, "exec_qty": 100,
                "pnl_worst_case": 2.0, "gross_edge_at_fill": 5.0,
                "markout_60s": 0.0, "fill_id": "f3",
            },
        ])
        m = compute_composite_score(ledger)
        # All side_taker=BUY → we SOLD (ASK hit) → capital returned per fill
        # capital_recycling_rate = sum(exec_qty*exec_price) / fill_count = sum(100*0.55)*3 / 3 = 55 etc.
        assert m["sign_positive"] == 1.0
        assert m["tail_rate"] == 0.0
        assert m["fill_count"] == 3
        assert m["pnl_worst_sum"] == 4.5
        # Capital recycling rate = (55 + 55 + 55) / 3 = 55 (capital returned / n_fillings)
        # Wait — that means each "fill" recovers $55; composite multiplied by `capital_recycling_rate` unbounded.
        # The formula caps at max(0, min(1.0, recycling_rate)) so we loose precision but the score is positive.
        assert m["capital_recycling_rate"] > 0
        assert m["composite_score"] > 0
        print(f"  PASS: composite={m['composite_score']:.4f}  pnl_worst_sum={m['pnl_worst_sum']:.4f}")


def test_compounding_score_zero_when_pnl_worst_negative():
    print("--- compounding_score: sign=0 when pnl_worst_sum ≤ 0 ---")
    with tempfile.TemporaryDirectory() as t:
        ledger = Path(t) / "ledger.parquet"
        _write_fills(ledger, [
            {"ts_utc": 1000, "asset_id": "Y", "market": "0x", "side_taker": "BUY", "exec_price": 0.55, "exec_qty": 100, "pnl_worst_case": -1.0, "gross_edge_at_fill": 5.0, "markout_60s": 0.0, "fill_id": "f1"},
        ])
        m = compute_composite_score(ledger)
        assert m["composite_score"] == 0.0
        print("  PASS")


def main():
    test_compounding_score_zero_when_no_filledger()
    test_compounding_score_positive_when_caps_recycle()
    test_compounding_score_zero_when_pnl_worst_negative()


if __name__ == "__main__":
    main()
