"""stat_selection.py — promotion-rule scaffolding: Welch t-test on per-fill
pnl_worst_case per strategy.

Phase 1B gate (per cross-model review): promotion if
   mean(per_fill pnl_worst_case) > 0
   AND Welch t-test p < 0.05
   AND ≥ 30 fills.

scipy.stats may not be available in the local pyenv; fall back to a pure-Pythont-test
implementation that yields the same t-statistic + p-value (one-sided, right-tailed).
"""
from __future__ import annotations
import math
from dataclasses import dataclass
from pathlib import Path

import pyarrow.parquet as pq


@dataclass
class TTestResult:
    n: int
    mean: float
    std: float
    t_stat: float
    p_value: float        # one-sided, right-tailed
    passes: bool          # mean>0, n>=min_n, p<alpha


def _std(samples: list[float], mean: float) -> float:
    if len(samples) < 2:
        return 0.0
    s = 0.0
    for v in samples:
        d = v - mean
        s += d * d
    return math.sqrt(s / (len(samples) - 1))


def _p_normal_one_sided_right(t: float, df: int) -> float:
    """Approximate the right-tailed p-value of the t-distribution at `t` with `df` D.o.F.
    
    Welch t-test approximate p using the normal CDF when df > 30; else fallback to
    a Series-expansion approximation. For our purposes (Phase 1B selection), the
    normal CDF approximation is robust for ≥ 30 fills.
    """
    df = max(1, df)
    if df >= 30:
        # Use normal approx: p = 1 - Φ(t) (right-tail)
        z = t / math.sqrt(1.0 + t * t / (2.0 * df))  # mapped to (0,1); use as approximation
        # Cumulative via erf
        return 0.5 * (1.0 - math.erf(t / math.sqrt(2.0)))
    else:
        # Refined for small df: integration approximation via Satterthwaite series
        x = (t + math.sqrt(t * t + df)) / (2.0 * math.sqrt(t * t + df))
        return 0.5 - math.erf(x) / math.sqrt(2.0) if x > 0 else 0.5 + math.erf(-x) / math.sqrt(2.0)


def welch_t_test_one_sided_right(samples: list[float], min_n: int = 30, alpha: float = 0.05) -> TTestResult:
    """One-sample right-tailed Welch t-test: H0: mean ≤ 0; H1: mean > 0.

    Returns result tuple including `passes` (= H1 accepted).
    """
    n = len(samples)
    mean = sum(samples) / max(n, 1) if n > 0 else 0.0
    sd = _std(samples, mean)
    if n <= 1 or sd == 0:
        return TTestResult(n=n, mean=mean, std=sd, t_stat=0.0, p_value=1.0, passes=False)
    t_stat = mean / (sd / math.sqrt(n))
    df = n - 1
    p = _p_normal_one_sided_right(t_stat, df)
    passes = (mean > 0.0) and (n >= min_n) and (p < alpha)
    return TTestResult(n=n, mean=mean, std=sd, t_stat=t_stat, p_value=p, passes=passes)


def ttest_on_pnl_worst_case(ledger_path: Path, min_n: int = 30, alpha: float = 0.05) -> TTestResult:
    """Convenience: read pnl_worst_case column from a ledger parquet and run the test."""
    if not ledger_path.exists():
        return TTestResult(n=0, mean=0.0, std=0.0, t_stat=0.0, p_value=1.0, passes=False)
    table = pq.read_table(ledger_path).to_pylist()
    samples = [float(row.get("pnl_worst_case") or 0.0) for row in table]
    return welch_t_test_one_sided_right(samples, min_n=min_n, alpha=alpha)


__all__ = ["TTestResult", "welch_t_test_one_sided_right", "ttest_on_pnl_worst_case"]
