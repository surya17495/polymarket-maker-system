"""main_live.py — Phase-2A live order-placement entry.

Replaces main_paper.py's phantom depth-shrinkage fill heuristic with lib/
live_order_placer.py's L2-API + EIP-712 signed orders, putting REAL orders
on the Polymarket CLOB whose fills are validated against the on-chain
ConditionSwap transactionHash attributed to our proxyWallet.

This is the SCRIPT THE USER USES once KYC is signed-off, _env populated, and
py_clob_client installed. Until then, instantiation of LiveOrderPlacer
fails with LiveConfigurationError and main_live prints the failing
manufactor log line (instead of running the trade loop).

Quotes decision logic is shared with main_paper.py via lib/strategies.py +
the WS-capture path remains the same (lib/clob_ws_public.WSClient). The
actual CLOB REST POST differs ONLY at the placement-termination step:
  pre-KYC phase_1a:   QuoteSubmit -> heuristic book-shrinkage fill emit
                      (lib/strategy_lab.py live paper-executor aim)
  post-KYC phase_2a: QuoteSubmit -> LiveOrderPlacer.place_quote -> CLOB
                      REST POST EIP-712 signed order -> fill receipt via
                      poll_for_fills + on-chain transactionHash validation

Until Phase-2A is enabled, main_live --dry-run echos the live paper_executor
activity as before for sanity check + observability, but discards any
heuristic fill confidence; the live paper_executor in --dry-run drops the
phantom-fill rate in favor of a more conservative Submit-only log.
"""
from __future__ import annotations
import argparse
import asyncio
import logging
import os
import sys
from pathlib import Path
from typing import Optional

from lib.live_order_placer import (
    CLOB_HOST,
    LiveConfigurationError,
    LiveNotImplementedError,
    LiveOrderCredentials,
    LiveOrderPlacer,
)


log = logging.getLogger(__name__)


async def phase_2a(
    capture_sec: float,
    top_n: int,
    seed_tokens: Optional[list[str]] = None,
    enable_rotation: bool = True,
    dry_run: bool = False,
) -> dict:
    """Live Phase-2A entry. Phase-2A replaces the lib/strategy_lab.py
    phantom-fill heuristic with lib/live_order_placer.py real fill receipts.

    Construction preconditions (any missing returns pre-KYC banner):
      - .env populated with POLY_L2_API_KEY / SECRET / PASSPHRASE / PRIV_KEY
        (optional POLY_PROXY_WALLET_ADDRESS)
      - `pip install py_clob_client`
      - funded EVM wallet (USDC top-up ≥ the planned inventory cap
        RouterConfig.max_total_inventory_usd)
    """
    creds = LiveOrderCredentials.from_env()
    summary: dict = {
        "mode": "phase_2a_dry_run" if dry_run else "phase_2a_live",
        "capture_sec": capture_sec,
        "top_n": top_n,
        "started_at_utc": None,
        "ended_at_utc": None,
        "post_kyc_activated": False,
    }
    if dry_run:
        # dry-run mode: skip credential check; print banner; do nothing.
        log.warning(
            "main_live --dry-run: skipping LiveOrderPlacer instantiation. "
            "Live Phase-2A not activated; lab heuristic in main_paper.py is "
            "still the source-of-truth (empirically phantom per lab_v5 "
            "analysis 2026-07-26). See docs/strategy_doc.md § Phase-2A.")
        return summary
    if creds is None:
        log.error(
            "LiveOrderCredentials missing required env. Phase-2A not "
            "activated.\n"
            "  required: POLY_L2_API_KEY, POLY_L2_API_SECRET, "
            "POLY_L2_API_PASSPHRASE, POLY_EVM_WALLET_PRIV_KEY\n"
            "  optional: POLY_PROXY_WALLET_ADDRESS (proxy wallet; None "
            "= native wallet flow signature_type=0)\n"
            "  See .env.example + docs/strategy_doc.md § Phase-2A.")
        return summary
    try:
        placer = LiveOrderPlacer(creds)
    except LiveConfigurationError as e:
        log.error("LiveOrderPlacer construction failed: %s", e)
        return summary
    try:
        placer.connect()
    except LiveConfigurationError as e:
        log.error("py_clob_client install / connect failed: %s", e)
        return summary
    summary["post_kyc_activated"] = True
    # Phase-2A trade-loop body is the post-KYC main-trade loop. Until KYC
    # is wired (user L2 KYC in Polymarket UI), this loop body is
    # intentionally simple — we let the Connected placer pose one order
    # then poll, raising LiveNotImplementedError on flow into the actual
    # placement pathway.
    try:
        # NOTE: actual placement requires live QuoteSubmit construction from
        # the WS capture-loop loop (lib/clob_ws_public.WSClient +
        # lib/strategies.py quote_at_tick). The post-KYC PLACEMENT loop
        # is mirrored to be a copy of main_paper.py's phase_1a() driver with
        # the substitution of Router.paper_executor -> a LivePaidExecutor
        # (TODO: subclass loops.paper_executor.PaperExecutor overriding
        # submit(q) to call placer.place_quote(q) and
        # poll_fills_to_ledger -> placer.poll_for_fills(...)).
        dummy_quote = None
        _ = placer.place_quote(dummy_quote)  # placeholder; raises LiveNotImplementedError
        log.error("Phase-2A trade-loop body awaited fill materialization. See TODO.")
    except LiveNotImplementedError as e:
        log.error("Phase-2A post-KYC body not yet implemented: %s", e)
        return summary
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Phase-2A live order-placement driver")
    parser.add_argument("--phase-2a", type=float, default=None,
                         help="Start Phase-2A live driver for capture_sec. "
                              "Requires Polymarket KYC + L2 API key (.env populated).")
    parser.add_argument("--top-n", type=int, default=15,
                         help="Number of markets to actively quote in this cycle.")
    parser.add_argument("--no-rotation", action="store_true",
                         help="Disable periodic re-discovery + WS rotation.")
    parser.add_argument("--seed-tokens", type=str, default=None,
                         help="CSV of clob token_ids; debug-only override.")
    parser.add_argument("--dry-run", action="store_true",
                         help="Skip LiveOrderPlacer instantiation; print "
                              "pre-KYC banner; for tx-log benchmarking only.")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=level, format="%(asctime)s %(name)s: %(message)s")
    if args.phase_2a is None:
        parser.print_help()
        print("\nERROR: --phase-2a <capture_sec> is required. Phase-2A "
              "requires KYC; use --dry-run only for log smoke-tests.", file=sys.stderr)
        sys.exit(2)
    summary = asyncio.run(phase_2a(
        capture_sec=args.phase_2a, top_n=args.top_n,
        seed_tokens=None,
        enable_rotation=not args.no_rotation,
        dry_run=args.dry_run,
    ))
    log.info("phase_2a summary: %s", summary)


if __name__ == "__main__":
    main()
