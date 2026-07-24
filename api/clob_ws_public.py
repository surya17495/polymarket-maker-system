"""clob_ws_public.py — Polymarket CLOB WebSocket subscribe client (no-auth).

Public endpoint: wss://ws-subscriptions-clob.polymarket.com/ws/market
Subscribe payload: {"assets_ids": [token_id, ...], "type": "market"}
Pushed messages:
  - event_type=book           (full book snapshot, one per asset on subscribe)
  - event_type=price_change   (per-level deltas with new size; size=0 = remove level)
  - event_type=tick_size_change
Each price_change carries: market (conditionId), price_changes: [{asset_id, price, size, side, hash, ...}]
side: "BUY" = bid side, "SELL" = ask side.

Reconnect with exponential backoff. Streams parsed messages to a callback
on_message(parsed) where parsed is a dict with keys: event_type, asset_id,
market, ts.
"""
from __future__ import annotations
import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable

import websockets

log = logging.getLogger(__name__)

DEFAULT_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
DEFAULT_CONNECT_TIMEOUT = 15.0
DEFAULT_RECV_TIMEOUT = 8.0
DEFAULT_RECONNECT_BACKOFF = 2.0
DEFAULT_MAX_BACKOFF = 60.0
DEFAULT_HEARTBEAT_PING = 20.0


MessageCallback = Callable[[dict], Awaitable[None] | None]


@dataclass
class WSClient:
    url: str = DEFAULT_WS_URL
    asset_ids: list[str] = field(default_factory=list)
    on_message: MessageCallback | None = None
    connect_timeout_sec: float = DEFAULT_CONNECT_TIMEOUT
    recv_timeout_sec: float = DEFAULT_RECV_TIMEOUT
    reconnect_backoff_sec: float = DEFAULT_RECONNECT_BACKOFF
    max_backoff_sec: float = DEFAULT_MAX_BACKOFF
    heartbeat_ping_sec: float = DEFAULT_HEARTBEAT_PING

    _ws: Any = None
    _stopping: bool = False
    _msg_count: int = 0
    _last_msg_t: float = 0.0
    _book_count: int = 0
    _pc_count: int = 0
    _connect_t: float = 0.0

    @property
    def stats(self) -> dict:
        return {
            "msg_count": self._msg_count,
            "book_count": self._book_count,
            "pc_count": self._pc_count,
            "connect_t_sec": self._connect_t,
            "last_msg_age_sec": (time.time() - self._last_msg_t)
            if self._last_msg_t
            else None,
        }

    async def _send_subscribe(self) -> None:
        sub = {"assets_ids": list(self.asset_ids), "type": "market"}
        await self._ws.send(json.dumps(sub))
        log.info("subscribed to %d assets", len(self.asset_ids))

    def _parse(self, raw: str | bytes) -> list[dict]:
        try:
            data = json.loads(raw)
        except Exception as e:
            log.warning("json parse failed: %s (raw=%r)", e, raw[:200])
            return []
        if isinstance(data, list):
            return list(data)
        if isinstance(data, dict):
            return [data]
        return []

    async def _handle_message(self, msg: dict) -> None:
        et = msg.get("event_type")
        ts = int(msg.get("timestamp") or 0)
        if et == "book":
            asset_id = msg.get("asset_id")
            market = msg.get("market")
            norm = {
                "event_type": "book",
                "asset_id": asset_id,
                "market": market,
                "ts": ts,
                "hash": msg.get("hash"),
                "tick_size": msg.get("tick_size"),
                "last_trade_price": msg.get("last_trade_price"),
                "bids": msg.get("bids", []),
                "asks": msg.get("asks", []),
                "ts_raw": ts,
                "recv_t_ms": int(time.time() * 1000),
            }
            self._book_count += 1
        elif et == "price_change":
            market = msg.get("market")
            ts_pc = int(msg.get("timestamp") or 0)
            changes = msg.get("price_changes") or []
            if not changes:
                return
            norm = {
                "event_type": "price_change",
                "market": market,
                "ts": ts_pc,
                "ts_raw": ts_pc,
                "recv_t_ms": int(time.time() * 1000),
                "changes": changes,
            }
            self._pc_count += 1
        elif et == "tick_size_change":
            norm = {
                "event_type": "tick_size_change",
                "asset_id": msg.get("asset_id"),
                "market": msg.get("market"),
                "ts": ts,
                "old_tick_size": msg.get("old_tick_size"),
                "new_tick_size": msg.get("new_tick_size"),
                "ts_raw": ts,
                "recv_t_ms": int(time.time() * 1000),
            }
        else:
            norm = {
                "event_type": et or "unknown",
                "raw": msg,
                "ts": ts,
                "ts_raw": ts,
                "recv_t_ms": int(time.time() * 1000),
            }
        self._msg_count += 1
        self._last_msg_t = time.time()
        if self.on_message is not None:
            cb = self.on_message(norm)
            if asyncio.iscoroutine(cb):
                await cb

    async def _run_once(self) -> None:
        t0 = time.time()
        async with websockets.connect(
            self.url, open_timeout=self.connect_timeout_sec
        ) as ws:
            self._ws = ws
            self._connect_t = time.time() - t0
            log.info("WS connected in %.2fs", self._connect_t)
            await self._send_subscribe()
            while not self._stopping:
                try:
                    raw = await asyncio.wait_for(
                        ws.recv(), timeout=self.recv_timeout_sec + self.heartbeat_ping_sec
                    )
                    for m in self._parse(raw):
                        await self._handle_message(m)
                except asyncio.TimeoutError:
                    try:
                        pong_waiter = await ws.ping()
                        await asyncio.wait_for(
                            pong_waiter, timeout=self.heartbeat_ping_sec
                        )
                    except Exception as e:
                        log.warning("heartbeat failed: %s; reconnecting", e)
                        return
                except websockets.ConnectionClosed as e:
                    log.warning("WS closed: %s; reconnecting", e)
                    return

    async def run_forever(self) -> None:
        backoff = self.reconnect_backoff_sec
        while not self._stopping:
            try:
                await self._run_once()
            except Exception as e:
                log.error("run_once failed: %s", e)
            if self._stopping:
                break
            log.info("sleeping %.1fs before reconnect", backoff)
            await asyncio.sleep(backoff)
            backoff = min(self.max_backoff_sec, backoff * 2)

    def stop(self) -> None:
        self._stopping = True

    def update_asset_ids(self, new_ids: list[str]) -> None:
        self.asset_ids = list(new_ids)


async def quick_smoke(
    asset_ids: list[str], listen_sec: float = 15.0
) -> dict:
    """One-off helper for connectivity tests. Returns stats dict."""
    stats = {"book_count": 0, "pc_count": 0, "msg_count": 0, "errors": []}

    async def on_msg(m: dict) -> None:
        stats["msg_count"] += 1
        if m["event_type"] == "book":
            stats["book_count"] += 1
        elif m["event_type"] == "price_change":
            stats["pc_count"] += 1

    cli = WSClient(asset_ids=list(asset_ids), on_message=on_msg)
    try:
        task = asyncio.create_task(cli.run_forever())
        await asyncio.sleep(listen_sec)
        cli.stop()
        try:
            await asyncio.wait_for(task, timeout=3.0)
        except asyncio.TimeoutError:
            task.cancel()
    except Exception as e:
        stats["errors"].append(repr(e))
    stats.update(cli.stats)
    return stats
