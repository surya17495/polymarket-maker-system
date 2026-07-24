"""walk_forward.py — splits a raw_events.jsonl capture into train/test windows.

For the Strategy Lab: tune on W_k, test on W_{k+1}. Prevents our strategy-sweep
from overfitting to one capture window.

Window splitting assumes the raw_events file is ORDERED by ts_raw (it is, since
it's append-only). Each window is a contiguous .jsonl file containing only events
whose ts_raw ∈ [start_ms, end_ms).
"""
from __future__ import annotations
import json
import logging
from pathlib import Path
from typing import Iterable

log = logging.getLogger(__name__)


def _read_raw_events(path: Path) -> list[dict]:
    out: list[dict] = []
    if not path.exists():
        return out
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                out.append(json.loads(line))
            except Exception:
                continue
    return out


def _ts_of(ev: dict) -> int:
    return int(ev.get("ts_raw") or ev.get("ts") or 0)


def split_capture_into_windows(
    raw_events_path: Path,
    output_dir: Path,
    window_minutes: int = 30,
) -> list[Path]:
    """Split raw_events.jsonl into N windows of `window_minutes` minutes each.
    
    Returns list of window file Paths ordered by ts_raw. Empty windows are skipped.
    """
    out: list[Path] = []
    events = _read_raw_events(raw_events_path)
    if not events:
        log.warning("no raw_events found at %s", raw_events_path)
        return out
    output_dir.mkdir(parents=True, exist_ok=True)
    t0 = _ts_of(events[0])
    if t0 == 0:
        log.warning("first raw event has ts_raw=0; cannot split windows")
        return out
    # Generate windows in chronological order
    window_ms = window_minutes * 60 * 1000
    last_ts = _ts_of(events[-1])
    n_windows = max(1, (last_ts - t0) // window_ms + 1)
    for w_idx in range(n_windows):
        start = t0 + w_idx * window_ms
        end = start + window_ms
        out_path = output_dir / f"window_{w_idx:03d}.jsonl"
        with open(out_path, "w") as w:
            n_w = 0
            for ev in events:
                if start <= _ts_of(ev) < end:
                    w.write(json.dumps(ev, default=str) + "\n")
                    n_w += 1
        if n_w:
            out.append(out_path)
        else:
            try:
                out_path.unlink()
            except Exception:
                pass
    log.info("split capture into %d windows (window_minutes=%d)", len(out), window_minutes)
    return out


def load_window_paths(window_dir: Path) -> list[Path]:
    return sorted(p for p in window_dir.iterdir() if p.name.startswith("window_") and p.suffix == ".jsonl")


def pairs_for_walk_forward(windows: list[Path]) -> list[tuple[Path | None, Path]]:
    """Yield (train_path, test_path) pairs: (W_0,W_1), (W_1,W_2), ...; for the first
    pair, train_path is None (initial state — no learning done in window 0)."""
    out: list[tuple[Path | None, Path]] = []
    prev: Path | None = None
    for w in windows:
        out.append((prev, w))
        prev = w
    return out


__all__ = ["split_capture_into_windows", "load_window_paths", "pairs_for_walk_forward"]
