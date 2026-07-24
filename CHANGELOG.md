# Changelog — Polymarket Maker System (polymarket_research)

All notable changes are documented here. Format follows Keep a Changelog; session-bucketed because the project's development happens in conversational sessions, not commits.

## WIP — 2026-07-24 session: Strategy Lab build + offline replay evaluator

### Added
- **`lib/strategy_lab.py`** — offline replay-based strategy evaluator running S0..S6 over the SAME `raw_events.jsonl` in seconds, emitting per-strategy `ledger_<sid>.parquet`, `merges_<sid>.parquet`, `summary_<sid>.parquet`, and a `lab_ranking.json`. Bypasses the live `paper_executor`'s O(N×M) buffer-scan with an event-indexed per-asset placement lookup. Supports `--walk-forward` window-split mode.
- **`lib/poly_estimators.py`** — port of `poly_maker/strategy/estimators.py`: time-decayed `Ewma`, `VolEstimator`, `FlowEstimator`, `MarkoutTracker`, `MarketEstimators`. Pure state machines; no I/O.
- **`lib/poly_quoting.py`** — adapted port of `poly_maker/strategy/quoting.py`: `construct_quotes` pure function with reservation `r = FV − skew`, half-spread `δ = base + c_vol·σ + c_tox·toxicity`, two-sided BUY-YES at `r − δ` and BUY-NO at `(1−r) − δ`, `post-only` placement guards, `_maybe_exit` SELL walk-to-touch by urgency.
- **`lib/poly_regime.py`** — port of `poly_maker/strategy/regime.py`: 5-state regime machine `HALTED > EVENT > REDUCE_ONLY > TRENDING > QUIET` with sweep cool-off + jump_ticks threshold. REDUCE_ONLY is the architectural fix for the inventory-saturation pathology discovered during live capture (router would saturate at max_inventory_per_market_usd because `quote_size_usd == max_inventory_per_market_usd` block the unwinding ASK).
- **`lib/poly_merger.py`** — per-`condition_id` MergerState + MergeEvent simulator: YES+NO join returns `pair_qty × (1 − p − q)` USDC of realized PnL to the deployable pool — capital recycling mechanism that does NOT depend on a taker lifting the unwinding ASK. Phase 2A will replace this with real on-chain EIP-712 batch via `poly_maker/merge.py` (EOA / Safe / V2 DepositWallet — verified live tx 0x4d2a2064 on LeBron neg-risk merge).
- **`lib/strategies.py`** — Strategy catalog S0..S6 cumulative layering: S0 BBTick baseline; S1 PolyQuoting; S2 = S1 + ReduceOnly regime; S3 = S2 + MergerState; S4 = S3 + anti-thrash; S5 = S4 + reverse-position; S6 = S5 + stop-loss RV + cooldown + take-profit. `strategy_factory(strategy_id, book_store, pair_map)` builder.
- **`lib/mirrored_book.py`** — `MirroredBookStore` auto-derives NO books from YES books via the binary-market invariant YES + NO = $1 (`NO best_bid = 1 − YES best_ask; NO best_ask = 1 − YES best_bid`). Lab reconstructs NO when the live capture subscribed to YES-only.
- **`lib/market_pairs.py`** — one-shot gamma-API fetcher caching `state/pair_map.parquet` (~59,230 token entries). Supplies `pair_map: dict[asset_id -> MarketPair]` enabling per-asset lookup to YES+NO complements.
- **`lib/trades_truth.py`** — `/trades` REST paginated fetcher + `TradeRecord` dataclass + `authoritative_taker_flow`. Cross-model arxiv 2604.24366 finding: book-derived trade direction is only 59-62% reliable; /trades is the authoritative ground-truth source.
- **`lib/compounding_score.py`** — composite compounding metric: `composite = sign(Σ pnl_worst > 0) × max(0, min(1, capital_recycling_rate)) × (1 − as_drag/gross) × (1 − tail_rate) × log(1 + fill_count)`. Bounded; positive only if pnl_worst positive AND recycling, low AS, low tail.
- **`lib/walk_forward.py`** — splits `raw_events.jsonl` into N chronological windows + emits (train, test) pairs.
- **`lib/stat_selection.py`** — Welch one-sample right-tailed t-test on `pnl_worst_case` per strategy. Pure-Python (no scipy required); promotion criterion: `mean > 0, n >= min_n, p < alpha`.
- Tests: `tests/test_poly_libs.py` (Ewma + Vol + Flow + Markout + round_to_tick + construct_quotes + Regime + Merger + MirroredBook), `tests/test_strategy_lab_smoke.py` (end-to-end smoke), `tests/test_compounding_score.py`, `tests/test_walk_forward.py`, `tests/test_stat_selection.py`. All PASSED as of 2026-07-24 23:00 UTC.

### Changed
- **`loops/router.py`** — refactored to add abstract `Strategy` ABC + `BBTickStrategy` (S0) default. `Router.decide_quote_submits` now accepts `now_ms / now_perf` overrides (used by the Strategy Lab to drive simulated time); signature backward-compatible (defaults to `time.time()`). Existing `paper_executor` unit tests still pass (5/5).
- **`docs/polymarket_maker_strategy_v1.md`** — appended §19 covering the Strategy Lab build, the strategy catalog, the empirical lab run on the live 48h rotating capture, and the first empirical Phase 1A signal (S1 +$4.21 pnl_worst_sum, Welch p<0.05 — profiled from a single 1.6h capture slice; full /trades ground-truth validation pending for §11 commit).

### Empirical Phase 1A signal (study ending 2026-07-24 22:50 UTC)

S1 (PolyQuoting) vs S0 (BBTick baseline) replayed against the same ~41k-msg raw_events.jsonl from the running 48h-rotating capture:
- **S1**: 100 fills, +$4.21 worst-case pnl sum, Welch t-test PASSES at p≈0 (n=100, mean=0.042, t=15.96)
- **S0**: 32 fills, $0.00 worst-case pnl sum (all `pnl_worst_case` are 0), Welch FAILS
- **S2-S6**: emit quotes but 0 fills (AntiThrash filter too aggressive + regime HALTED dominates on soon-to-resolve esports markets; debuggable in next session)

Caveat: fills detected via the WS depth-shrinkage heuristic, which has a ~40% noise floor (per arxiv 2604.24366: book-derived trade direction is only 59-62% reliable vs on-chain ground truth). The `+$4.21` is an upper bound until the `/trades` ground-truth integration (next session) is wired into the lab to validate each simulated fill.

Section 11 (Predicted returns — PENDING EMPIRICAL MEASUREMENT) anchors were the Sonnett tail-weighted $1-$6/day (low) vs Centri optimistic $50-$300/day (high), both NULL HYPOTHESES awaiting measurement. The S1 +$4.21/100-fills/1.6h extrapolates to a ~$63/day plausible ceiling (sits between the two anchors) — NOT yet committed to §11 as that requires /trades validation first.

## [2026-07-24 earlier — strategy doc v3] 

See docs/polymarket_maker_strategy_v1.md §17 Phase 0 build + §18 activity-filtered scanner + 48h Phase 1A capture launched for the prior session's record.
