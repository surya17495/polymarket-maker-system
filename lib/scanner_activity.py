"""scanner_activity.py — WS activity probe for currently-active market filtering.

Subscribes briefly to top-N candidate asset_ids, counts price_change messages
per asset, returns the per-asset message counts. Used by Loop A scanner to
prefer actively-trading markets over dormant-but-high-spread ones (e.g.,
dormant esports map rounds vs live BTC monthlies).

Designed to run AFTER Loop A's static ranking drops a pass_count_top set,
then probe broadly; many candidates will be silent. Re-rank afterwards.
"""
from __future__ import annotations
import asyncio
import logging
import os
import sys
import time
from collections import defaultdict
from typing import Awaitable, Callable

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from api.clob_ws_public import WSClient

log = logging.getLogger(__name__)


async def probe_ws_activity(asset_ids: list[str], listen_sec: float) -> dict[str, int]:
    """Subscribe to all `asset_ids`; over `listen_sec` seconds, count price_change
    messages per asset_id (as identified by the per-change `asset_id` field).
    Returns: {asset_id: msg_count}"""
    counts: dict[str, int] = defaultdict(int)

    async def on_msg(m: dict) -> None:
        et = m.get("event_type")
        if et == "price_change":
            for c in m.get("changes") or []:
                aid = c.get("asset_id")
                if aid:
                    counts[aid] += 1
        elif et == "book":
            aid = m.get("asset_id")
            if aid:
                counts[aid] += 0  # book snapshot isn't activity

    cli = WSClient(asset_ids=list(asset_ids), on_message=on_msg)
    task = asyncio.create_task(cli.run_forever())
    log.info("WS activity probe: %d assets for %.0fs", len(asset_ids), listen_sec)
    await asyncio.sleep(listen_sec)
    cli.stop()
    try:
        await asyncio.wait_for(task, timeout=5.0)
    except (asyncio.TimeoutError, asyncio.CancelledError):
        task.cancel()
    return dict(counts)
