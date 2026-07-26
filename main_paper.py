"""main_paper.py — Phase 0 + Phase 1A driver.

Opcodes:
  --once                Loop A discovery only. Outputs state/candidates_ranked.parquet.
  --capture-sec N       Discovery + WS-only capture (Phase 0 raw events instrument).
  --phase-1a N          Full Phase 1A integrated capture (Loop A + Loop B + Loop C + Loop E).
                        Runs end-to-end: WS sub + BookStore + Router + PaperExecutor + daily summary.
                        Outputs:
                          - state/candidates_ranked.parquet  (Loop A)
                          - state/raw_events.jsonl           (raw WS events)
                          - state/ledger.parquet             (per-fill via PaperExecutor)
                          - state/daily_summary.parquet      (Loop E aggregation)
                          - state/latency_summary.json       (p50/p95/p99)
                          - state/phase1a_run_summary.json   (run recap: scan_id, asset_ids,
                                                            fills, quotes submitted, etc.)
"""
from __future__ import annotations
import argparse
import asyncio
import json
import logging
import os
import shutil
import sys
import time
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import pyarrow.parquet as pq
import yaml

from api.clob_ws_public import WSClient
from lib.book import BookStore
from lib.latency_model import get_latency_stats
from loops.discovery import LoopA, load_scan_config_from_yaml
from loops.paper_executor import PaperExecutor
from loops.router import Router, load_router_config_from_yaml
from loops.analytics import aggregate_daily_summary, backfill_markout_60s_into_ledger

log = logging.getLogger("main_paper")

HERE = Path(__file__).resolve().parent
CONFIG_PATH = HERE / "config.yaml"
STATE_DIR = HERE / "state"
CANDIDATES_PATH = STATE_DIR / "candidates_ranked.parquet"
RAW_EVENTS_PATH = STATE_DIR / "raw_events.jsonl"
LEDGER_PATH = STATE_DIR / "ledger.parquet"
DAILY_SUMMARY_PATH = STATE_DIR / "daily_summary.parquet"
LATENCY_SUMMARY_PATH = STATE_DIR / "latency_summary.json"
RUN_SUMMARY_PATH = STATE_DIR / "phase1a_run_summary.json"


def setup_paths(preserve_existing: bool = True) -> None:
    """Initialize state directory and ensure capture-related files exist.

    Default behaviour 2026-07-26 (after "main_paper.py destroyed the 22h of
    capture" incident): keep existing raw_events.jsonl + ledger.parquet +
    phase1a_run_summary.json intact, no truncation. The previous behaviour
    was ``p.unlink()`` on each (raw events jsonl + ledger + run summary) at
    startup which permanently destroyed prior capture when the live paper
    trader was started fresh — counter to user expectation that "more data
    is always better" and that restarts accumulate rather than reset.

    When `preserve_existing=False` is explicitly passed (= old behaviour),
    raw_events.jsonl is moved to a timestamped archive before truncating
    so the old capture is recoverable rather than unrecoverable-on
    filesystem.
    """
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    ts = time.strftime("%Y%m%d_%H%M%S")
    archive_dir = STATE_DIR / "archive"
    for p in (RAW_EVENTS_PATH, LEDGER_PATH, RUN_SUMMARY_PATH):
        if not p.exists():
            continue
        if preserve_existing:
            # Move-to-archive + leave the file in place (with old content
            # preserved). The live capture appends with `"a"` mode so
            # subsequent writes are appended next to existing events. The
            # dedicated `phase1a_run_summary.json` heartbeat persists from
            # the PREVIOUS capture run but the running process writes a
            # fresh heartbeat on top of it within the next latency-heartbeat
            # interval (default 15s).
            archive_dir.mkdir(parents=True, exist_ok=True)
            archive_p = archive_dir / f"{p.name}.{ts}.pre_capture.bak"
            try:
                shutil.copy2(p, archive_p)
                log.info("backed up %s -> %s (preserve mode)", p.name, archive_p.name)
            except Exception as exc:
                log.warning("backup of %s failed: %s", p, exc)
        else:
            # Explicit fresh-state mode: archive-with-move-then-truncate.
            archive_dir.mkdir(parents=True, exist_ok=True)
            archive_p = archive_dir / f"{p.name}.{ts}.pre.archive"
            try:
                p.replace(archive_p)
                log.info("archived %s -> %s (fresh-state mode)", p.name, archive_p.name)
            except Exception as exc:
                log.warning("archive of %s failed: %s", p, exc)
            log.debug("removed %s (fresh-state mode)", p)


def load_top_asset_ids_from_parquet(n: int) -> list[str]:
    if not CANDIDATES_PATH.exists():
        return []
    table = pq.read_table(CANDIDATES_PATH)
    df = table.to_pylist()
    asset_ids = []
    for row in df[:n]:
        aid = row.get("asset_id")
        if aid:
            asset_ids.append(aid)
    return asset_ids


def load_all_config() -> dict:
    with open(CONFIG_PATH) as f:
        return yaml.safe_load(f)


async def router_tick_loop(
    router: Router,
    asset_ids: list[str],
    paper_executor: PaperExecutor,
    scan_id: str,
    tick_sec: int,
    stop_at_perf_counter: float,
    stats_rec: dict,
) -> None:
    while time.perf_counter() < stop_at_perf_counter:
        try:
            quotes = router.decide_quote_submits(scan_id, asset_ids)
            for q in quotes:
                placed_id = paper_executor.submit_quote(q)
                stats_rec["quote_submits_total"] += 1
        except Exception as exc:
            log.warning("router tick failed: %s", exc)
        await asyncio.sleep(tick_sec)


async def paper_executor_loop(pe: PaperExecutor, stop_at_perf_counter: float) -> None:
    while time.perf_counter() < stop_at_perf_counter:
        try:
            await pe._process_pending_quotes()
            await pe._process_book_events_into_fills()
        except Exception as exc:
            log.warning("paper_executor tick failed: %s", exc)
        await asyncio.sleep(0.05)


async def phase_1a(
    capture_sec: float,
    top_n: int,
    include_initial_discovery: bool = True,
    seed_tokens: list[str] | None = None,
    enable_rotation: bool = True,
    preserve_existing_state: bool = True,
) -> dict:
    """Periodic-re-discovery Phase 1A capture.

    When enable_rotation=True (default), spawns a periodic Loop A scan_loop
    task that re-runs discovery every `scan_cycle_sec` and rotates the WS
    subscribe set to the new top-N when ranks change. Router + PaperExecutor
    continue running across rotations: pending quotes for dropped tokens
    are expired; BookStore entries for dropped tokens are popped to bound memory.

    When enable_rotation=False, falls back to one-shot + frozen subscribe (v1 behavior).

    `preserve_existing_state` (default True, set False only with explicit
    `--fresh-state` CLI flag) -> see setup_paths(): refuses to truncate
    raw_events.jsonl / ledger / run_summary; archives them to state/archive/
    instead. Default-preserving behaviour added 2026-07-26 after the
    accidental-22h-capture-truncation incident.
    """
    setup_paths(preserve_existing=preserve_existing_state)
    cfg_yaml = load_all_config()
    scan_cfg = load_scan_config_from_yaml(str(CONFIG_PATH))
    router_cfg = load_router_config_from_yaml(str(CONFIG_PATH))
    scan_cycle_sec = scan_cfg.scan_cycle_sec

    latency = get_latency_stats()
    book_store = BookStore()
    router = Router(cfg=router_cfg, book_store=book_store)
    paper_executor = PaperExecutor(
        book_store=book_store,
        router=router,
        raw_events_path=RAW_EVENTS_PATH,
        fill_log_path=LEDGER_PATH,
        default_latency_sample_ms=int(cfg_yaml.get("ws", {}).get("heartbeat_ping_sec", 20)) * 0 + 240,
        kill_switch_drawdown_pct_warn=float(cfg_yaml["kill_switches"]["drawdown_warn_pct"]),
        kill_switch_drawdown_pct_reduce=float(cfg_yaml["kill_switches"]["drawdown_reduce_pct"]),
        kill_switch_drawdown_pct_halt=float(cfg_yaml["kill_switches"]["drawdown_halt_pct"]),
        base_equity_usd=2000.0,
    )
    base_scan_id = "phase1a_" + ("rot_" if enable_rotation else "frz_") + str(int(time.time()))
    raw_fh = open(RAW_EVENTS_PATH, "a", encoding="utf-8")
    stats_rec = {
        "quote_submits_total": 0,
        "raw_msgs_total": 0,
        "book_count": 0,
        "price_change_count": 0,
        "scan_count": 0,
        "rotation_count": 0,
    }
    perf_t0 = time.perf_counter()
    stop_at_perf = perf_t0 + capture_sec
    state = {
        "active_asset_ids": [],
        "cli": None,
        "ws_task": None,
    }

    async def on_message(msg: dict) -> None:
        line = json.dumps(msg, default=str)
        raw_fh.write(line + "\n")
        raw_fh.flush()
        stats_rec["raw_msgs_total"] += 1
        et = msg.get("event_type")
        if et == "book":
            book_store.apply_ws_message(msg)
            stats_rec["book_count"] += 1
        elif et == "price_change":
            t_apply = time.perf_counter()
            book_store.apply_ws_message(msg)
            paper_executor.ingest_ws_message(msg)
            latency.record_ws_apply(t_apply, time.perf_counter())
            latency.record_ws_detect(int(msg.get("recv_t_ms") or 0), int(msg.get("ts_raw") or 0))
            stats_rec["price_change_count"] += 1

    async def rotate_ws(new_active: list[str]) -> None:
        old = list(state["active_asset_ids"])
        to_drop = [a for a in old if a not in new_active]
        to_add = [a for a in new_active if a not in old]
        if not to_drop and not to_add:
            log.info("rotation: no change (still %d active)", len(new_active))
            return
        stats_rec["rotation_count"] += 1
        log.info("rotation %d: drop=%d add=%d total_new=%d",
                 stats_rec["rotation_count"], len(to_drop), len(to_add), len(new_active))
        for placed_id, p in list(paper_executor.placed_quotes.items()):
            if p.quote.asset_id in to_drop:
                paper_executor.placed_quotes.pop(placed_id, None)
        for aid in to_drop:
            book_store.books.pop(aid, None)
        if state["cli"] is not None:
            state["cli"].stop()
        if state["ws_task"] is not None:
            try:
                await asyncio.wait_for(state["ws_task"], timeout=3.0)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                try:
                    state["ws_task"].cancel()
                except Exception:
                    pass
        if not new_active:
            log.warning("rotation: new asset set empty; pausing until next scan")
            state["cli"] = None
            state["ws_task"] = None
            state["active_asset_ids"] = []
            return
        cli = WSClient(asset_ids=list(new_active), on_message=on_message)
        state["cli"] = cli
        state["active_asset_ids"] = list(new_active)
        state["ws_task"] = asyncio.create_task(cli.run_forever())

    async def scan_loop():
        """Periodic Loop A scan + rotation — runs every scan_cycle_sec."""
        while time.perf_counter() < stop_at_perf:
            stats_rec["scan_count"] += 1
            scan_iter = stats_rec["scan_count"]
            log.info("scan cycle %d: running Loop A discovery...", scan_iter)
            try:
                loop_a = LoopA(cfg=scan_cfg, out_path=str(CANDIDATES_PATH))
                rows = await loop_a.run_once_async()
                log.info("scan cycle %d: %d ranked candidates", scan_iter, len(rows))
                if rows:
                    new_active = load_top_asset_ids_from_parquet(top_n)
                    await rotate_ws(new_active)
            except Exception as e:
                log.warning("scan cycle %d failed: %s", scan_iter, e)
            if time.perf_counter() >= stop_at_perf:
                break
            await asyncio.sleep(scan_cycle_sec)

    async def router_tick_loop_state(scan_id_local: str):
        while time.perf_counter() < stop_at_perf:
            try:
                active = list(state["active_asset_ids"])
                if not active:
                    await asyncio.sleep(10)
                    continue
                quotes = router.decide_quote_submits(scan_id_local, active)
                for q in quotes:
                    paper_executor.submit_quote(q)
                    stats_rec["quote_submits_total"] += 1
            except Exception as e:
                log.warning("router tick failed: %s", e)
            await asyncio.sleep(60)

    # Initial scan + WS sub BEFORE spawning the periodic scan_loop
    if seed_tokens:
        log.info("Phase 1A: starting with seed_tokens (%d assets)", len(seed_tokens))
        loop_a = LoopA(cfg=scan_cfg, out_path=str(CANDIDATES_PATH))
        try:
            await loop_a.run_once_async()
        except Exception as e:
            log.warning("initial scan failed (with seed_tokens, non-fatal): %s", e)
        initial_active = [str(a) for a in seed_tokens][:top_n]
        await rotate_ws(initial_active)
        log.info("Phase 1A: WS subscribe to seed %d assets; capture %.0fs", len(initial_active), capture_sec)
    elif include_initial_discovery:
        log.info("=== phase_1a: initial Loop A discovery ===")
        loop_a = LoopA(cfg=scan_cfg, out_path=str(CANDIDATES_PATH))
        rows = await loop_a.run_once_async()
        initial_active = load_top_asset_ids_from_parquet(top_n)
        await rotate_ws(initial_active)
        log.info("=== phase_1a: discovery complete; %d rows, %d active subscribed ===", len(rows), len(initial_active))
    elif CANDIDATES_PATH.exists():
        initial_active = load_top_asset_ids_from_parquet(top_n)
        await rotate_ws(initial_active)
        log.info("Phase 1A: loaded %d initial active from existing parquet", len(initial_active))
    else:
        raise RuntimeError(f"missing {CANDIDATES_PATH} (run discovery first)")

    if not state["active_asset_ids"]:
        log.warning("no active asset_ids after initial scan; aborting capture")
        RUN_SUMMARY_PATH.write_text(
            json.dumps({"error": "no active asset_ids", "base_scan_id": base_scan_id}, indent=2, default=str)
        )
        return {"base_scan_id": base_scan_id, "active_asset_ids": []}

    # Spawn tasks: scanner loop (if rotation enabled), router tick, paper executor
    scan_task = None
    if enable_rotation:
        scan_task = asyncio.create_task(scan_loop())
    router_task = asyncio.create_task(router_tick_loop_state(base_scan_id))
    pe_task = asyncio.create_task(paper_executor_loop(paper_executor, stop_at_perf))

    async def latency_heartbeat():
        """Every 15s: write latency_summary.json + partial run_summary.json so dashboard
        reflects live state while capture is still running. Without this, the dashboard
        only sees post-capture summary."""
        while time.perf_counter() < stop_at_perf:
            try:
                LATENCY_SUMMARY_PATH.write_text(
                    json.dumps(latency.summary(), indent=2, default=str)
                )
                partial_summary = {
                    "mode": "phase_1a_rotating" if enable_rotation else "phase_1a_frozen",
                    "scan_id_base": base_scan_id,
                    "capture_sec": capture_sec,
                    "stats_rec": stats_rec,
                    "paper_executor": {
                        "placed_quotes_remaining": len(paper_executor.placed_quotes),
                        "completed_fills_count": len(paper_executor.completed_fills),
                    },
                    "active_asset_ids": state["active_asset_ids"],
                    "captured_so_far_sec": int(time.perf_counter() - perf_t0),
                    "is_running": True,
                    "ended_at_utc": None,
                }
                RUN_SUMMARY_PATH.write_text(
                    json.dumps(partial_summary, indent=2, default=str)
                )
            except Exception as e:
                log.warning("heartbeat failed: %s", e)
            await asyncio.sleep(15)

    heartbeat_task = asyncio.create_task(latency_heartbeat())
    log.info("Phase 1A: capture live. rotation=%s scan_cycle_sec=%d top_n=%d capture_sec=%d",
             enable_rotation, scan_cycle_sec, top_n, int(capture_sec))

    try:
        await asyncio.sleep(capture_sec)
    except asyncio.CancelledError:
        log.info("capture cancelled by user")
        pass

    log.info("capture period complete; cleaning up")
    if state["cli"]:
        state["cli"].stop()
    for t in [router_task, pe_task, scan_task, heartbeat_task]:
        if t is None:
            continue
        try:
            await asyncio.wait_for(t, timeout=3.0)
        except (asyncio.TimeoutError, asyncio.CancelledError):
            try:
                t.cancel()
            except Exception:
                pass
    if state["ws_task"]:
        try:
            await asyncio.wait_for(state["ws_task"], timeout=3.0)
        except (asyncio.TimeoutError, asyncio.CancelledError):
            try:
                state["ws_task"].cancel()
            except Exception:
                pass
    raw_fh.close()

    try:
        n_mk = backfill_markout_60s_into_ledger(LEDGER_PATH, RAW_EVENTS_PATH, window_sec=60)
        log.info("backfilled markout_60s in %d ledger rows", n_mk)
    except Exception as e:
        log.warning("markout backfill failed: %s", e)

    flushed = paper_executor.flush_fills_to_parquet()
    log.info("flushed %d fill rows to ledger", flushed)

    n_summary = aggregate_daily_summary(LEDGER_PATH, DAILY_SUMMARY_PATH)
    log.info("Loop E daily summary: %d rows", n_summary)

    latency_summary = latency.summary()
    LATENCY_SUMMARY_PATH.write_text(json.dumps(latency_summary, indent=2, default=str))

    run_summary = {
        "mode": "phase_1a_rotating" if enable_rotation else "phase_1a_frozen",
        "scan_id_base": base_scan_id,
        "asset_ids_initial": state["active_asset_ids"],
        "capture_sec": capture_sec,
        "stats_rec": stats_rec,
        "router_quote_submits_total": stats_rec["quote_submits_total"],
        "paper_executor": {
            "placed_quotes_remaining": len(paper_executor.placed_quotes),
            "completed_fills_count": len(paper_executor.completed_fills),
        },
        "fill_log_rows": flushed,
        "daily_summary_rows": n_summary,
        "latency_summary": latency_summary,
        "ended_at_utc": time.time(),
    }
    RUN_SUMMARY_PATH.write_text(json.dumps(run_summary, indent=2, default=str))
    try:
        if n_summary > 0:
            ds = pq.read_table(DAILY_SUMMARY_PATH).to_pylist()
            total_exp_pnl = sum(float(r.get("expected_pnl_sum") or 0.0) for r in ds)
            run_summary["aggregate_expected_pnl_usd"] = total_exp_pnl
            tier = paper_executor.check_kill_switches(realized_pnl_usd=total_exp_pnl, deployed_usd=200.0)
            run_summary["kill_switch_final_audit"] = tier
    except Exception as e:
        log.warning("kill-switch audit failed: %s", e)

    return run_summary


async def capture_only(capture_sec: float, top_n: int, preserve_existing_state: bool = True) -> dict:
    """Phase 0 raw capture only — no router, no paper_executor (kept for legacy)."""
    setup_paths(preserve_existing=preserve_existing_state)

    if not CANDIDATES_PATH.exists():
        loop_a = LoopA(out_path=str(CANDIDATES_PATH))
        loop_a.run_once()
    asset_ids = load_top_asset_ids_from_parquet(top_n)
    if not asset_ids:
        return {"top_n": top_n, "asset_ids": []}
    latency = get_latency_stats()
    book_store = BookStore()
    raw_fh = open(RAW_EVENTS_PATH, "a", encoding="utf-8")
    counters = {"book": 0, "pc": 0, "msg": 0}

    async def on_message(msg: dict) -> None:
        raw_fh.write(json.dumps(msg, default=str) + "\n")
        raw_fh.flush()
        counters["msg"] += 1
        if msg.get("event_type") == "book":
            counters["book"] += 1
            book_store.apply_ws_message(msg)
            latency.record_ws_detect(int(msg.get("recv_t_ms") or 0), int(msg.get("ts_raw") or 0))
        elif msg.get("event_type") == "price_change":
            counters["pc"] += 1
            book_store.apply_ws_message(msg)
            latency.record_ws_apply(time.perf_counter(), time.perf_counter())

    cli = WSClient(asset_ids=asset_ids, on_message=on_message)
    task = asyncio.create_task(cli.run_forever())
    await asyncio.sleep(capture_sec)
    cli.stop()
    try:
        await asyncio.wait_for(task, timeout=5.0)
    except asyncio.TimeoutError:
        task.cancel()
    raw_fh.close()
    latency_summary = latency.summary()
    LATENCY_SUMMARY_PATH.write_text(json.dumps(latency_summary, indent=2, default=str))
    return {
        "asset_ids": asset_ids, "msg_count": counters["msg"],
        "book_count": counters["book"], "pc_count": counters["pc"],
        "latency_summary": latency_summary,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Phase 0 / Phase 1A driver")
    parser.add_argument("--once", action="store_true", help="Loop A discovery only")
    parser.add_argument("--capture-sec", type=float, default=None,
                        help="Phase 0 raw WS capture only (no router/paper_executor)")
    parser.add_argument("--phase-1a", type=float, default=None,
                        help="Phase 1A integrated capture (Loop B+C+E) for the specified seconds")
    parser.add_argument("--top-n", type=int, default=5)
    parser.add_argument("--no-discovery", action="store_true",
                        help="reuse existing state/candidates_ranked.parquet instead of re-running Loop A at start")
    parser.add_argument("--seed-tokens", type=str, default=None,
                        help="comma-separated list of clob_asset_ids to override scanner pick " \
                             "(debug / known-active-market override)")
    parser.add_argument("--no-rotation", action="store_true",
                         help="disable periodic re-discovery + WS rotation (one-shot + frozen subscribe; legacy v1 behavior)")
    parser.add_argument("--fresh-state", action="store_true",
                         help="(2026-07-25 fix) explicit override: truncate raw_events.jsonl "
                              "+ ledger + run_summary on startup. Without this flag, the "
                              "live capture now ARCHIVES existing state to state/archive/ "
                              "and PRESERVES raw_events.jsonl for cross-run accumulation "
                              "(was the default 2026-07-25 - reversed after 'that's "
                              "the whole point of the live capture' feedback). Default "
                              "False (preserve existing).")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=level, format="%(asctime)s %(name)s: %(message)s")

    if args.phase_1a is not None:
        seed_tokens = None
        if args.seed_tokens:
            seed_tokens = [t.strip() for t in args.seed_tokens.split(",") if t.strip()]
        summary = asyncio.run(
            phase_1a(
                capture_sec=args.phase_1a, top_n=args.top_n,
                include_initial_discovery=not args.no_discovery,
                seed_tokens=seed_tokens,
                enable_rotation=not args.no_rotation,
                preserve_existing_state=not args.fresh_state,
            )
        )
    elif args.capture_sec is not None:
        summary = asyncio.run(capture_only(args.capture_sec, args.top_n, preserve_existing_state=not args.fresh_state))
    elif args.once:
        loop_a = LoopA(out_path=str(CANDIDATES_PATH))
        rows = loop_a.run_once()
        summary = {"ranked_rows": len(rows)}
    else:
        log.info("no mode specified; defaulting to --once")
        loop_a = LoopA(out_path=str(CANDIDATES_PATH))
        rows = loop_a.run_once()
        summary = {"ranked_rows": len(rows)}

    print(json.dumps(summary, default=str, indent=2))


if __name__ == "__main__":
    main()
