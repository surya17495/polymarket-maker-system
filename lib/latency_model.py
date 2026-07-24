"""latency_model.py — WS detect latency + REST submit latency sampler.

Phase 0 / 1A require we measure end-to-end reaction time for the maker.
We separately log:
  - WS_detection_lag_ms   = recv_t_ms (python) - ts_raw (servername)
  - REST_submit_lag_ms    = submit_request roundtrip - estimated round-trip
                           minus server processing time approximated by
                           ts_t delta
  - WS_to_book_apply_ms    = time from ws.recv() return to book.apply_change()

Phase 0 instrument populates a rolling stats singleton; later phases use
these exposures to feed the kill-switch and QoS gauges.
"""
from __future__ import annotations
import threading
import time
from collections import deque
from dataclasses import dataclass, field


@dataclass
class LatencyStats:
    max_samples: int = 1000

    ws_detect_ms: deque = field(default_factory=lambda: deque(maxlen=1000))
    ws_apply_ms: deque = field(default_factory=lambda: deque(maxlen=1000))
    rest_book_ms: deque = field(default_factory=lambda: deque(maxlen=1000))

    _lock: threading.Lock = field(default_factory=threading.Lock)

    def record_ws_detect(self, recv_t_ms: int, ts_raw_ms: int) -> None:
        delta = recv_t_ms - ts_raw_ms
        with self._lock:
            if 0 <= delta < 10_000:
                self.ws_detect_ms.append(delta)

    def record_ws_apply(self, apply_t_perf_counter: float, recv_t_perf_counter: float) -> None:
        delta_ms = (recv_t_perf_counter - apply_t_perf_counter) * 1000.0
        with self._lock:
            if 0 <= delta_ms < 10_000:
                self.ws_apply_ms.append(delta_ms)

    def record_rest_book_ms(self, ms: float) -> None:
        with self._lock:
            self.rest_book_ms.append(ms)

    def summary(self) -> dict:
        def stats(d: deque) -> dict:
            if not d:
                return {"n": 0, "p50": None, "p95": None, "p99": None, "max": None}
            with self._lock:
                arr = sorted(d)
            n = len(arr)
            def _p(pct):
                idx = int(n * pct) - 1
                idx = max(0, min(n - 1, idx))
                return arr[idx]
            return {
                "n": n,
                "p50": _p(0.50),
                "p95": _p(0.95),
                "p99": _p(0.99),
                "max": arr[-1],
            }
        return {
            "ws_detect_ms": stats(self.ws_detect_ms),
            "ws_apply_ms": stats(self.ws_apply_ms),
            "rest_book_ms": stats(self.rest_book_ms),
        }


_LATENCY_STATS: LatencyStats | None = None


def get_latency_stats() -> LatencyStats:
    global _LATENCY_STATS
    if _LATENCY_STATS is None:
        _LATENCY_STATS = LatencyStats()
    return _LATENCY_STATS
