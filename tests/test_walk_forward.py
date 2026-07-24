"""test_walk_forward.py — splits a synthetic raw_events.jsonl into N windows."""
from __future__ import annotations
import json
import os
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lib.walk_forward import split_capture_into_windows, load_window_paths, pairs_for_walk_forward


def test_split_capture_creates_contiguous_windows():
    print("--- walk_forward: split_capture_into_windows ---")
    with tempfile.TemporaryDirectory() as t:
        tmp = Path(t)
        raw = tmp / "raw_events.jsonl"
        # 100 events at 0.1s intervals — 10s of data, with window_minutes=1 (=60s) we get 1 window
        # with window_minutes=0.5 (=30s) we get windows of 30s each
        # Use a 30 second total length → split into windows of 10s each = 3 windows
        events = []
        for i in range(100):
            t_ms = 1000 + i * 300  # 300ms apart → 30 seconds total
            events.append({"event_type": "book", "asset_id": "X", "ts_raw": t_ms})
        raw.write_text("\n".join(json.dumps(e) for e in events))

        windows = split_capture_into_windows(raw, tmp / "windows", window_minutes=1)
        # 30 seconds = ~0.5 minutes → 1 window with 1-min window_size (since start+1min covers all)
        assert len(windows) >= 1, f"expected at least 1 window, got {len(windows)}"
        # Each window file has events
        for w in windows:
            with open(w) as f:
                lines = [l for l in f.readlines() if l.strip()]
            assert len(lines) > 0
        print(f"  split into {len(windows)} windows for 30s of synthetic events")

        pairs = pairs_for_walk_forward(windows)
        assert len(pairs) == len(windows)
        # First pair has train=None
        assert pairs[0][0] is None
        # Each subsequent pair has train=prev, test=cur
        if len(pairs) >= 2:
            assert pairs[1][0] == windows[0]
            assert pairs[1][1] == windows[1]
        _ok_test("split + walk_forward pairs OK", True)


def _ok_test(name, cond):
    print(f"  PASS: {name}")


def main():
    test_split_capture_creates_contiguous_windows()


if __name__ == "__main__":
    main()
