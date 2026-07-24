"""discovery.py — Loop A scanner.

Reuses gamma + REST /book + enriched_score to enumerate and rank candidate
markets. Output: state/candidates_ranked.parquet with the top-N weighted
candidates. Callable from main_paper.py and main_live.py.

Loop A is the discovery-side component; it runs every scanner.scan_cycle_sec
(default 5 minutes). Each cycle:
  1. fetch_events (gamma max_events=2500 via 25 pages x 100)  -> list[gamma event dict]
  2. per market with vol>=vol_24h_min_usd and bestBid>0, fetch /book
  3. compute enriched_score
  4. (NEW) OPTIONALLY probe WS activity over `ws_probe_sec` seconds — counts
     price_change messages per asset_id. Multiply enriched_score by
     ws_activity_factor (our activity_factorbucks). Markets with NO recent
     price_changes get activity_factor=0 (discarded — dormant).
  5. keep top pass_count_top (default 30) and write parquet

The parquet has one row per market; it includes:
  asset_id (yes_token), condition_id, event_slug, event_title,
  event_vol24h, market_question, best_bid_price, best_ask_price, spread_c,
  inside_depth_usd, top5_depth_usd, fee_type, neg_risk, end_date,
  base_opportunity, enriched_score, ws_activity_count, activity_score,
  scored_at_utc, scan_id
"""
from __future__ import annotations
import asyncio
import datetime as _dt
import logging
import os
import sys
import time
import uuid
from dataclasses import dataclass, field
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import yaml

from api.clob_rest_public import ClobRestClient
from api.gamma import GammaClient, normalize_market
from lib.enriched_score import EnrichedScorer, days_to_end, infer_topic_key

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from lib.scanner_activity import probe_ws_activity

log = logging.getLogger(__name__)


def load_scan_config_from_yaml(path: str) -> ScanConfig:
    with open(path) as f:
        y = yaml.safe_load(f)
    s = y.get("scanner") or {}
    return ScanConfig(
        scan_cycle_sec=int(s.get("scan_cycle_sec", 300)),
        gamma_max_events=int(s.get("gamma_max_events", 2500)),
        gamma_page_size=int(s.get("gamma_page_size", 100)),
        vol_24h_min_usd=float(s.get("vol_24h_min_usd", 200)),
        vol_24h_max_usd=(float(s["vol_24h_max_usd"]) if (s.get("vol_24h_max_usd") not in (None, "")) else None),
        req_timeout_sec=float(s.get("req_timeout_sec", 8)),
        req_parallelism=int(s.get("req_parallelism", 20)),
        pass_count_top=int(s.get("pass_count_top", 30)),
        min_inside_depth_usd=float(s.get("min_inside_depth_usd", 0.5)),
        min_spread_c=float(s.get("min_spread_c", 0.5)),
        ws_probe_sec=float(s.get("ws_probe_sec", 0)),
        ws_probe_top_n=int(s.get("ws_probe_top_n", 100)),
    )


@dataclass
class ScanConfig:
    scan_cycle_sec: int = 300
    gamma_max_events: int = 2500      # bumped: pilgrimage the full universe (~2100 events)
    gamma_page_size: int = 100         # gamma hard-caps at 100 per request
    vol_24h_min_usd: float = 200.0
    vol_24h_max_usd: float | None = None
    req_timeout_sec: float = 8.0
    req_parallelism: int = 20
    pass_count_top: int = 30
    min_inside_depth_usd: float = 0.5
    min_spread_c: float = 0.5
    ws_probe_sec: float = 0.0           # 0 = disabled
    ws_probe_top_n: int = 100          # probe top-N candidates after scoring


@dataclass
class LoopA:
    cfg: ScanConfig = field(default_factory=ScanConfig)
    gamma: GammaClient = field(default_factory=GammaClient)
    rest: ClobRestClient = field(default_factory=ClobRestClient)
    scorer: EnrichedScorer = field(default_factory=EnrichedScorer)
    out_path: str = "state/candidates_ranked.parquet"

    def _filter_event_vol(self, ev: dict) -> bool:
        try:
            v = float(ev.get("volume24hr") or 0)
        except (TypeError, ValueError):
            return False
        if v < self.cfg.vol_24h_min_usd:
            return False
        if self.cfg.vol_24h_max_usd and v > self.cfg.vol_24h_max_usd:
            return False
        return True

    def _collect_candidates(self) -> list[dict]:
        events = self.gamma.fetch_events(max_events=self.cfg.gamma_max_events)
        log.info("gamma returned %d events", len(events))
        cands: list[dict] = []
        for ev in events:
            if not self._filter_event_vol(ev):
                continue
            ev_slug = ev.get("slug") or ""
            ev_title = ev.get("title") or ""
            neg_risk = bool(ev.get("negRisk") or False)
            for m in ev.get("markets", []) or []:
                try:
                    mv = float(m.get("volume") or m.get("volumeNum") or 0)
                except (TypeError, ValueError):
                    mv = 0.0
                if mv < self.cfg.vol_24h_min_usd * 0.1:
                    pass
                bb = m.get("bestBid") or m.get("best_bid")
                ba = m.get("bestAsk") or m.get("best_ask")
                if not (bb and ba):
                    continue
                try:
                    bb_f = float(bb)
                    ba_f = float(ba)
                except (TypeError, ValueError):
                    continue
                if bb_f <= 0 or ba_f <= 0 or ba_f <= bb_f:
                    continue
                ci = m.get("conditionId")
                tokens_raw = m.get("clobTokenIds")
                if isinstance(tokens_raw, str):
                    import json as _j
                    try:
                        tokens_raw = _j.loads(tokens_raw)
                    except Exception:
                        tokens_raw = [tokens_raw]
                if not tokens_raw:
                    continue
                yes_token = str(tokens_raw[0]) if isinstance(tokens_raw, list) else str(tokens_raw)
                cands.append({
                    "asset_id": yes_token,
                    "condition_id": ci,
                    "event_slug": ev_slug,
                    "event_title": ev_title,
                    "event_neg_risk": neg_risk,
                    "event_end_date": ev.get("endDate") or m.get("endDate"),
                    "event_vol24h": float(ev.get("volume24hr") or 0),
                    "market_question": m.get("question", ""),
                    "market_vol": mv,
                    "best_bid_price": bb_f,
                    "best_ask_price": ba_f,
                    "spread_c": (ba_f - bb_f) * 100.0,
                    "fee_type": m.get("feeType") or "default",
                    "fees_enabled": bool(m.get("feesEnabled") or False),
                })
        log.info("collected %d candidate markets from gamma", len(cands))
        return cands

    async def _fetch_books_concurrent(self, cands: list[dict]) -> None:
        if not cands:
            return
        sem = asyncio.Semaphore(self.cfg.req_parallelism)
        client = self.rest

        async def probe(c: dict) -> None:
            async with sem:
                book = await asyncio.to_thread(client.fetch_book, c["asset_id"])
            if not book:
                return
            bids = book.get("bids") or []
            asks = book.get("asks") or []
            if not (bids and asks):
                return
            try:
                bb = float(bids[0]["price"])
                bb_sz = float(bids[0]["size"])
                ba = float(asks[0]["price"])
                ba_sz = float(asks[0]["size"])
            except (TypeError, ValueError, KeyError, IndexError):
                return
            inside_bid_usd = bb_sz * bb
            inside_ask_usd = ba_sz * ba
            inside_depth_usd = inside_bid_usd + inside_ask_usd
            top5_bid_usd = 0.0
            for b in bids[:5]:
                try:
                    top5_bid_usd += float(b["size"]) * float(b["price"])
                except (TypeError, ValueError, KeyError):
                    continue
            top5_ask_usd = 0.0
            for a in asks[:5]:
                try:
                    top5_ask_usd += float(a["size"]) * float(a["price"])
                except (TypeError, ValueError, KeyError):
                    continue
            c["book_best_bid"] = bb
            c["book_best_ask"] = ba
            c["book_best_bid_size"] = bb_sz
            c["book_best_ask_size"] = ba_sz
            c["inside_depth_usd"] = inside_depth_usd
            c["inside_bid_usd"] = inside_bid_usd
            c["inside_ask_usd"] = inside_ask_usd
            c["top5_depth_usd"] = top5_bid_usd + top5_ask_usd
            c["spread_c"] = (ba - bb) * 100.0

        await asyncio.gather(*(probe(c) for c in cands))

    def _score_candidates(self, cands: list[dict], scan_id: str, scored_at: str) -> list[dict]:
        out: list[dict] = []
        for c in cands:
            if c.get("inside_depth_usd") is None:
                continue
            if c.get("inside_depth_usd", 0) < self.cfg.min_inside_depth_usd:
                continue
            if c.get("spread_c", 0) < self.cfg.min_spread_c:
                continue
            dte = days_to_end(c.get("event_end_date"))
            topic_key = infer_topic_key(c.get("market_question") or c.get("event_title"))
            score_out = self.scorer.score(
                event_vol24h_usd=c["event_vol24h"],
                spread_c=c["spread_c"],
                inside_depth_usd=c["inside_depth_usd"],
                best_bid_size=c.get("book_best_bid_size", 0.0),
                best_ask_size=c.get("book_best_ask_size", 0.0),
                fee_type=c["fee_type"],
                neg_risk=c["event_neg_risk"],
                days_to_end=dte,
                topic_key=topic_key,
            )
            row = dict(c)
            row.update(score_out)
            row["topic_key"] = topic_key
            row["days_to_end"] = dte
            row["scan_id"] = scan_id
            row["scored_at_utc"] = scored_at
            row["ws_activity_count"] = 0
            row["activity_score"] = float(row.get("enriched_score") or 0.0)
            out.append(row)
        out.sort(key=lambda r: -r["enriched_score"])
        return out[: self.cfg.pass_count_top * 5]  # probe pre-rank wider set

    async def _run_ws_activity_probe(self, ranked: list[dict]) -> dict[str, int]:
        if self.cfg.ws_probe_sec <= 0 or not ranked:
            return {}
        top_ids = [r["asset_id"] for r in ranked[: self.cfg.ws_probe_top_n]]
        return await probe_ws_activity(top_ids, listen_sec=self.cfg.ws_probe_sec)

    def _apply_activity_filter(self, ranked: list[dict], activity: dict[str, int]) -> list[dict]:
        if not activity:
            return ranked[: self.cfg.pass_count_top]
        for r in ranked:
            count = activity.get(r["asset_id"], 0)
            r["ws_activity_count"] = count
            # activity_factor: log-saturating; cuts dead markets to ~0 weight
            # _max if 10 msgs / 15s considered "very active"; keep quadratic-softmax
            count_normalized = float(count)
            if count_normalized <= 0:
                # dead markets effectively suppressed
                r["activity_score"] = 0.0
                continue
            activity_factor = (1.0 + count_normalized) / (1.0 + max(activity.values()) + 1.0)
            r["activity_score"] = r["enriched_score"] * activity_factor * 50.0
        ranked.sort(key=lambda r: -r["activity_score"])
        return ranked[: self.cfg.pass_count_top]

    def _write_parquet(self, rows: list[dict], path: str) -> None:
        if not rows:
            log.warning("no rows to write; not creating %s", path)
            return
        flat = []
        for r in rows:
            try:
                flat.append({
                    "asset_id": r["asset_id"],
                    "condition_id": r.get("condition_id"),
                    "event_slug": r.get("event_slug"),
                    "event_title": r.get("event_title"),
                    "market_question": r.get("market_question"),
                    "event_vol24h": r.get("event_vol24h", 0.0),
                    "market_vol": r.get("market_vol", 0.0),
                    "best_bid_price": float(r.get("book_best_bid") or r.get("best_bid_price") or 0.0),
                    "best_ask_price": float(r.get("book_best_ask") or r.get("best_ask_price") or 0.0),
                    "best_bid_size": float(r.get("book_best_bid_size") or 0.0),
                    "best_ask_size": float(r.get("book_best_ask_size") or 0.0),
                    "spread_c": float(r.get("spread_c") or 0.0),
                    "inside_depth_usd": float(r.get("inside_depth_usd") or 0.0),
                    "inside_bid_usd": float(r.get("inside_bid_usd") or 0.0),
                    "inside_ask_usd": float(r.get("inside_ask_usd") or 0.0),
                    "top5_depth_usd": float(r.get("top5_depth_usd") or 0.0),
                    "fee_type": r.get("fee_type"),
                    "fees_enabled": bool(r.get("fees_enabled", False)),
                    "neg_risk": bool(r.get("event_neg_risk", False)),
                    "event_end_date": r.get("event_end_date"),
                    "days_to_end": r.get("days_to_end"),
                    "topic_key": r.get("topic_key"),
                    "base_opportunity": float(r.get("base_opportunity") or 0.0),
                    "balance_factor": float(r.get("balance_factor") or 0.0),
                    "fee_factor": float(r.get("fee_factor") or 0.0),
                    "neg_risk_factor": float(r.get("neg_risk_factor") or 0.0),
                    "res_factor": float(r.get("res_factor") or 0.0),
                    "as_factor": float(r.get("as_factor") or 0.0),
                    "enriched_score": float(r.get("enriched_score") or 0.0),
                    "ws_activity_count": int(r.get("ws_activity_count") or 0),
                    "activity_score": float(r.get("activity_score") or 0.0),
                    "scan_id": r.get("scan_id"),
                    "scored_at_utc": r.get("scored_at_utc"),
                })
            except Exception as exc:
                log.warning("row drop: %s", exc)
        if not flat:
            return
        table = pa.Table.from_pylist(flat)
        pq.write_table(table, path)
        log.info("wrote %d ranked rows to %s", len(flat), path)

    def run_once(self) -> list[dict]:
        scan_id = uuid.uuid4().hex[:12]
        scored_at = _dt.datetime.now(_dt.timezone.utc).isoformat()
        cands = self._collect_candidates()
        if not cands:
            return []
        try:
            asyncio.run(self._fetch_books_concurrent(cands))
        except RuntimeError:
            raise
        ranked = self._score_candidates(cands, scan_id, scored_at)
        if self.cfg.ws_probe_sec > 0:
            try:
                activity = asyncio.run(self._run_ws_activity_probe(ranked))
            except RuntimeError:
                log.warning("ws_probe skipped: cannot run asyncio.run from inside event loop")
                activity = {}
            ranked = self._apply_activity_filter(ranked, activity)
        else:
            ranked = ranked[: self.cfg.pass_count_top]
        self._write_parquet(ranked, self.out_path)
        return ranked

    async def run_once_async(self) -> list[dict]:
        """Async version. Do NOT call this from a synchronous asyncio.run scope."""
        scan_id = uuid.uuid4().hex[:12]
        scored_at = _dt.datetime.now(_dt.timezone.utc).isoformat()
        cands = self._collect_candidates()
        if not cands:
            return []
        await self._fetch_books_concurrent(cands)
        ranked = self._score_candidates(cands, scan_id, scored_at)
        if self.cfg.ws_probe_sec > 0:
            activity = await self._run_ws_activity_probe(ranked)
            ranked = self._apply_activity_filter(ranked, activity)
        else:
            ranked = ranked[: self.cfg.pass_count_top]
        self._write_parquet(ranked, self.out_path)
        return ranked

    async def run_loop(self, cycles: int | None = None, on_cycle_done=None) -> None:
        n = 0
        while cycles is None or n < cycles:
            t0 = time.time()
            try:
                rows = self.run_once()
                log.info("scan %d done: %d rows produced", n, len(rows))
                if on_cycle_done:
                    cb = on_cycle_done(rows)
                    if asyncio.iscoroutine(cb):
                        await cb
            except Exception as exc:
                log.error("scan %d failed: %s", n, exc)
            n += 1
            elapsed = time.time() - t0
            sleep_sec = max(self.cfg.scan_cycle_sec - elapsed, 5.0)
            if cycles is not None and n >= cycles:
                break
            await asyncio.sleep(sleep_sec)
