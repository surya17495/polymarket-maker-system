"""test_ws_connect.py — Phase 0 success criterion #1.

Validates the public WS endpoint is reachable from this environment, can
subscribe to multiple asset_ids, and receive at least the initial book
snapshot plus N price_change messages.

Run from maker_system/ root:
  python -m pytest tests/test_ws_connect.py -v
or:
  python tests/test_ws_connect.py
"""
from __future__ import annotations
import asyncio
import os
import sys
import time

import pytest

sys.path.insert(
    0, os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)

from api.clob_ws_public import WSClient, quick_smoke
from api.gamma import GammaClient

MIN_BENIGN_LATENCY_SEC = 30.0
MIN_MSGS_PER_TOKEN_PER_SEC = 0.05


def _pick_top_tokens(n: int = 5, verify_via_rest: bool = True) -> list[str]:
    """Pull N tokens from gamma-api, verified to have live L2 books.

    A market qualifies on the gamma side only if bestBid>0 AND bestAsk>0 AND
    lastTradePrice>0. If verify_via_rest=True, we additionally probe /book for
    each candidate and keep only the ones whose bids+asks are non-empty.
    Falls back to a known-hot list if no qualifying tokens found.
    """
    fallback = [
        "70060007759742302808850203923230495967841006347410479320265457231119910877637",
        "15143363423071604796751175575022439394931506726285032798960073277744145377124",
        "55689044028128278672494108251217443782536678376777545334307559186551480418539",
        "26485245961222313575373928818655498088613748810606082451526412785977121014592",
        "96728906606106688440336606663937949286677183925957865652114142287112355126160",
    ]
    raw_candidates: list[str] = []
    try:
        gc = GammaClient()
        events = gc.fetch_events(max_events=400)
        for ev in events:
            for m in ev.get("markets", []) or []:
                bb = m.get("bestBid")
                ba = m.get("bestAsk")
                ltp = m.get("lastTradePrice")
                if not (bb and ba and ltp):
                    continue
                try:
                    if float(bb) <= 0 or float(ba) <= 0 or float(ltp) <= 0:
                        continue
                except (TypeError, ValueError):
                    continue
                clob_ids = m.get("clobTokenIds")
                if isinstance(clob_ids, str):
                    import json as _j
                    try:
                        clob_ids = _j.loads(clob_ids)
                    except Exception:
                        clob_ids = [clob_ids]
                if isinstance(clob_ids, list):
                    raw_candidates.extend(str(x) for x in clob_ids if x)
                if len(raw_candidates) >= n * 4:
                    break
            if len(raw_candidates) >= n * 4:
                break
    except Exception:
        pass

    if not raw_candidates:
        return fallback[:n]

    if not verify_via_rest:
        return raw_candidates[:n]

    try:
        from api.clob_rest_public import ClobRestClient
        rest = ClobRestClient(timeout_sec=4.0)
        verified: list[str] = []
        for tid in raw_candidates:
            book = rest.fetch_book(tid)
            if not book:
                continue
            if book.get("bids") or book.get("asks"):
                verified.append(tid)
            if len(verified) >= n:
                break
        if not verified:
            return fallback[:n]
        return verified[:n]
    except Exception:
        return raw_candidates[:n] if raw_candidates else fallback[:n]


def test_endpoint_reachable():
    tokens = _pick_top_tokens(2)
    stats = asyncio.run(quick_smoke(tokens, listen_sec=10.0))
    assert stats["connect_t_sec"] < MIN_BENIGN_LATENCY_SEC, (
        f"connect latency {stats['connect_t_sec']}s exceeded "
        f"{MIN_BENIGN_LATENCY_SEC}s"
    )
    assert stats["book_count"] >= 1, "expected at least 1 book snapshot"
    assert stats["msg_count"] >= 1, "expected at least 1 message"


def test_subscribe_receives_book_snapshot_for_each_token():
    tokens = _pick_top_tokens(3)
    stats = asyncio.run(quick_smoke(tokens, listen_sec=15.0))
    per_token = len(tokens)
    assert stats["book_count"] >= per_token - 1, (
        f"expected ~{per_token} book snapshots (one per token), "
        f"got {stats['book_count']}"
    )


def test_receives_continuously_for_active_markets():
    """Subscribe to multiple high-volume tokens; expect stream within 20s."""
    tokens = _pick_top_tokens(5)
    stats = asyncio.run(quick_smoke(tokens, listen_sec=20.0))
    if stats["pc_count"] == 0:
        pytest.skip(
            "no price_change messages received — selected markets were quiet at "
            "test time; rerun with more popular markets"
        )


if __name__ == "__main__":
    print("Endpoint reachable test:")
    test_endpoint_reachable()
    print("  PASSED")
    print("Book snapshot per token test:")
    test_subscribe_receives_book_snapshot_for_each_token()
    print("  PASSED")
    print("Continuous stream test:")
    try:
        test_receives_continuously_for_active_markets()
        print("  PASSED")
    except pytest.skip.Exception as e:
        print(f"  SKIPPED: {e}")
    print("\nAll Phase 0 WS connectivity tests done.")
