"""test_stat_selection.py — unit tests for the Welch one-sided t-test."""
from __future__ import annotations
import os
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pyarrow as pa
import pyarrow.parquet as pq

from lib.stat_selection import welch_t_test_one_sided_right, ttest_on_pnl_worst_case


def test_welch_positive_mean_passes():
    print("--- Welch t-test: positive mean passes with enough samples ---")
    samples = [0.01, 0.02, 0.015, 0.012, 0.011, 0.014, 0.018, 0.013, 0.016, 0.020]
    samples = samples * 4  # pad to n=40
    r = welch_t_test_one_sided_right(samples, min_n=30, alpha=0.05)
    assert r.n >= 30
    assert r.mean > 0
    assert r.passes, f"expected to pass: mean={r.mean} t={r.t_stat} p={r.p_value}"
    print(f"  PASS: n={r.n} mean={r.mean:.4f} t={r.t_stat:.4f} p={r.p_value:.4f}")


def test_welch_negative_mean_does_not_pass():
    print("--- Welch t-test: negative mean fails ---")
    samples = [-0.01, -0.005, -0.008, -0.012, -0.007] * 10  # 50 negative samples
    r = welch_t_test_one_sided_right(samples, min_n=30, alpha=0.05)
    assert not r.passes, f"negative mean must fail, but passes: {r}"
    print("  PASS")


def test_welch_small_n_does_not_pass():
    print("--- Welch t-test: n<N_min fails ---")
    samples = [0.05, 0.06, 0.07]  # 3 samples — too few
    r = welch_t_test_one_sided_right(samples, min_n=30, alpha=0.05)
    assert not r.passes
    print("  PASS")


def test_ttest_on_pnl_worst_case_offline():
    print("--- ttest_on_pnl_worst_case from a synthetic ledger ---")
    with tempfile.TemporaryDirectory() as t:
        ledger = Path(t) / "ledger.parquet"
        # Mock a minimal "ledger" with one column: pnl_worst_case
        rows = [{"pnl_worst_case": 0.01 + i * 0.001} for i in range(40)]
        pq.write_table(pa.Table.from_pylist(rows), ledger)
        r = ttest_on_pnl_worst_case(ledger, min_n=30, alpha=0.05)
        assert r.n == 40
        assert r.passes
        print(f"  PASS: n={r.n} mean={r.mean:.4f} t={r.t_stat:.4f} p={r.p_value:.4f}")


def main():
    test_welch_positive_mean_passes()
    test_welch_negative_mean_does_not_pass()
    test_welch_small_n_does_not_pass()
    test_ttest_on_pnl_worst_case_offline()


if __name__ == "__main__":
    main()
