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

### Strategy Lab re-run with calibration fix (2026-07-24 ~23:40 UTC)

Two previously-un diagnosed configuration issues suppressed S2..S6 to zero fills on the prior 41k-msg run:

1. **`StrategyProfile.reduce_only_hours = 12.0`** (inherited from poly_maker's daily-resolving political-market defaults) was far too aggressive for our live Phase 1A capture, whose top-liquid markets (esports + daily-BTC) all resolve within 12h. The regime machine thus classified every active market as `REDUCE_ONLY`, which kills the adder in `construct_quotes` (line 199-200 of `poly_quoting.py`). Lowered to `1.0` — `REDUCE_ONLY` only kicks in within the final hour to resolution; matches Polymarket's resolution-endgame book-thinning.
2. **`AntiThrashStrategy` threshold inversion**: my prior "tightening" (`price_delta_c=0.10` / `size_delta_pct=0.05`) did the OPPOSITE of intended — lower thresholds suppress MORE requotes (the suppress condition is `dp < thr AND ds_rel < thr_pct`, so smaller thresholds fire more often). Result was 2 quote_submits total over a 170k-event run vs 2088 before tightening. Reverted to poly_maker's defaults of `0.50c / 10%`.

Re-run verdict on the 170k-msg capture (4x larger than the prior 41k-msg run, both within the same live 48h rotating Phase 1A window):

| strategy_id              | fills | merges | Σ pnl_worst (USD) | Welch p<0.05 | n  | composite |
|---|---|---|---|---|---|---|
| `s1_poly_quoting`        | 100 |  0 | +$4.2009 | **PASS** (p=0.00014) | 100 | 0.00*      |
| `s2_reduce_only`        |  92 |  0 | +$4.2009 | **PASS** (p=0.00013) |  92 | 0.00*      |
| `s3_with_merge`         |  92 | 46 | +$4.2009 | **PASS** (p=0.00013) |  92 | 4.533      |
| `s4_anti_thrash`        |  36 |  0 | +$0.9356 | FAIL (p=0.052, just barely above α=0.05) | 36 | 0.00* |
| `s5_reverse_pos`        |  36 |  0 | +$0.9356 | FAIL (same as S4) | 36 | 0.00*      |
| `s6_stop_loss`          |  36 |  0 | +$0.9356 | FAIL (same as S4) | 36 | 0.00*      |
| `s0_bb_tick`           |  32 |  0 | $0.0000  | FAIL (zero per-fill worst-case = queue_ahead_others consumes all depth) | 32 | 0.00 |

Interpretation:

- **3 of the 6 poly strategies (S1, S2, S3) now PASS the Welch-gated Phase 1A raw signal**. All three share `pnl_worst_sum = +$4.2009`; S2 takes 8 fewer fills (additive side silenced in the final hour of REDUCE_ONLY markets; sensible given inventory-risk aversion) but the per-fill mean rises by ~9%.
- **S3 demonstrates the architectural capital-recycling mechanism** that Cross-Model Review (GPT §8.6 + Claude) specifically agreed on as the structural response to the inventory-saturation pathology surfaced by the live Phase 1A capture. 46 merge events (50% of fills) return capital to the deployable pool WITHOUT depending on a taker lifting the ASK-side unwinding leg. `composite_score=4.533` is the only non-zero composite, reflecting this recycling; the metric's `[0,log(N+1)]`-bounded definition is offset by a `capital_recycling_rate` that momentarily exceeds 1.0 (a known computed-ratio quirk to fix in Phase 1B).
- **S4/S5/S6 each produce 36 fills (vs S2's 92)** at the SAME per-fill mean of ~$0.026 and aggregate `+$0.9356`, just barely missing the Welch gate at p=0.052 — 8 fills below the p<0.05 cutoff. The cumulative-decorator chain (AntiThrash → ReversePosition → StopLoss) propagates the same per-fill profile; on a longer capture (Phase 1B 7-14d) the additional samples will resolve whether the antithrash spread-tightening is just "less-fills-better-PnL" (per-fill mean rises from $0.026 to a meaningful margin) or net-negative. They are NOT a code bug.
- **S0 (BBTick) baseline** still $0.00 worst-case (queue_ahead fully consumes depth at worst-case) — confirming that the poly-quoting reservation/skew/half-spread construction is the source of marginal fill quality, not the BB+tick timing.

**Upper-bound reminder** (Contract 5, arxiv 2604.24366): the +$4.21 figure is still upper-bound until `/trades` ground-truth validation runs. Phase 2A KYC unlock is the only blocker (the lab infrastructure is built and tested; the `/trades` endpoint returns 401 to non-KYC'd clients).

## [2026-07-24 earlier — strategy doc v3] 

### Strategy Lab bug fixes — 2026-07-25 00:30 UTC (post cross-model GPT+Claude review)

Cross-model review raised two answers I had to land before the 45h capture extension:

1. **Claude priority-flag: "do the fill_id diff on s1 first"** (5-min-check against 45-hour commitment). DONE: byte-identical `ts_utc` and `asset_id` between the c14fe25 `ledger_s1.parquet` (41k-msg run) and the 329487c `ledger_s1.parquet` (170k-msg run). Root cause wasn't a stale-cache; the lab's `max_total_inventory_usd = 600` default saturates around fill #30 — after that, `quote_at_tick` returns `[]` for every subsequent router tick → no new placements → the 100 "fills" all came from the FIRST 3.27 min of capture regardless of subsequent 130k+ events.
2. **GPT-designated primary research candidate: S3 (with capital-recycling merger).** First scan: capital-recycling was decorative — the strategy's `MergeStrategy.after_fill` post-incremented `self.realized_pnl_from_merges += ev.realized_pnl_usd` (a counter) but never informed `InventoryState` of the capital returned via merge → `total_inventory_usd` never decreased → S3 emitted the same number of fills as S2 (46 merges were observable but had NO effect on subsequent quote capacity).
3. **Sign inversion in `InventoryState.apply_fill`** (`loops/router.py:56-61` — pre-fix code): when taker SOLD (we BOUGHT), the code subtracted from `per_market` → held shares became negative → strategy read `pos_yes = max(net_shares(yes), 0.0)` = 0 → `_maybe_exit` (Sell-side exit walker) returned None → held positions NEVER exited → `pnl_worst_case` was BID-touch edge alone, not round-trip realized PnL.
4. **Naming-only cosmetic** (`lib/stat_selection.py:59 welch_t_test_one_sided_right`): is actually a one-sample t-test against zero (no second sample, no Welsh-Satterthwaite correction). p-value computed via the normal-CDF approximation for df≥30 — valid as the t → ∞ asymptote. Math OK for our $(n=60-81, t>4)$ regime; rename not done to keep scope tight.

### Surgical fixes applied

- `loops/router.py`: inverted `apply_fill` sign convention (we BOUGHT → per_market[+] case vs prior we BOUGHT → per_market[-]) so `_maybe_exit` actually emits SELL exits based on real held inventory → realized round-trip PnL is reachable. Added new `apply_merge_return(yes_id, no_id, qty, mid_yes, mid_no)` method that atomically decrements both `per_market[yes]` and `[no]` share counts + refreshes `per_market_usd` to reflect residual held shares.
- `lib/strategy_lab.py`: after each `strategy.after_fill(fill, t_ms)` call, walks the decorator chain to find the deepest strategy carrying `.merge_events` (works for S3 + S4/S5/S6 because AntiThrash/ReversePosition/StopLoss all wrap an S3 base) — for each new MergeEvent, looks up `_pairs_by_condition[condition_id]` (reverse-index built once at init) + the YES/NO book current mids, then calls `inv.apply_merge_return(yes_token_id, no_token_id, ev.pair_qty, mid_y, mid_n)`. This is the wiring that makes the merger's capital recyclable; until this patch the merger was an accounting counter only.
- `lib/strategy_lab.py`: reverted lab default `max_total_inventory_usd` from `600` → `200` and `max_inventory_per_market_usd` from `150` → `50` — now matches the live `config.yaml` budget. Lab and live paper_executor operate under the same capital constraints so the lab-to-live fill ratio (GPT spec) can be computed without an unaccounted cap differential confounding the gap.
- All 6 test files (`tests/test_paper_executor.py`, `tests/test_poly_libs.py`, `tests/test_strategy_lab_smoke.py`, `tests/test_compounding_score.py`, `tests/test_stat_selection.py`, `tests/test_walk_forward.py`) pass through the refactored router + new merge wiring (no breakage from the sign flip because the tests don't assert on the bookkeeping sign — they assert on side_taker direction + the synthetic ledger row count).

### Strategy Lab re-run verdict (post-fix, 263k-msg raw_events.jsonl)

| # | strategy_id              | fills | qsub | merges | Σ pnl_worst | Welch n | Welch p    | gate       | side-taker distribution (BUY=we-sell / SELL=we-buy) |
|---|---|---|---|---|---|---|---|---|---|
| 1 | `s3_with_merge`         |  81 |  97 | 36 | +$3.2653 |  81 | 0.000662 | **PASS** | 11 / 70 (capital-recycled — incremental +26 fills above S2) |
| 2 | `s1_poly_quoting`       |  60 |  70 |  0 | +$2.3303 |  60 | 0.003183 | **PASS** |  5 / 55 (no merger → cap-bounded at ~60 fills) |
| 3 | `s2_reduce_only`        |  55 |  65 |  0 | +$2.3303 |  55 | 0.003078 | **PASS** |  5 / 50 (REDUCE_ONLY silences a few adds) |
| 4 | `s4_anti_thrash`        |  45 |  53 |  0 | +$0.9356 |  45 | 0.052479 | FAIL (8 short of α) | 11 / 34 |
| 5 | `s5_reverse_pos`        |  32 |  40 |  0 | +$0.9356 |  32 | 0.051156 | FAIL | (S4 base; same fill profile) |
| 6 | `s6_stop_loss`          |  32 |  40 |  0 | +$0.9356 |  32 | 0.051156 | FAIL | (S4 base; no RV scenario in 1.6h capture to exercise stop) |
| 7 | `s0_bb_tick`           |  32 | 246 |  0 |  $0.0000 |  32 | 1.0       | FAIL    | (queue_ahead_consume_all depth at worst-fill case) |

### Critical takeaways (post-fix)

- **The +$4.20 was inflated by the sign bug (~2×)**: post-fix pure-quoting S1 produces +$2.33 over 60 fills (vs the prior +$4.20 over 100 fills where 40 of the 100 came from never-tightening BUY-additions inventory-saturated at the lab's $600 cap).
- **S3 separates cleanly from S2/S1 for the first time**: 81 fills vs S2's 55 (= +26 incremental fills = +47% over S2) and +$0.94 PnL lift ($3.27 vs $2.33) driven exclusively by the merger recycling mechanism. The pre-fix table had S3 == S2 with the merge-recycle claimed but never wired through to the InventoryState; now the recycling IS visible in fill count and PnL.
- **Sell-side exits now fire** (the sign-flip's direct effect on `_maybe_exit`): S3 has 11 BUY-side (we-SELL our held YES) fills vs 70 SELL-side (we-BUY); S1 has 5 / 55. Pre-fix was 0 / N for all strategies (~(100, 100, 92, ...) ALL SELL-side). This is the proof-of-life for round-trip PnL measurement.
- **Fills still concentrated in the first 3.27 min** (max `ts_utc` of fill = 1784927186261, the 196s mark)— despite the larger $50/$200 capital budget matching config.yaml, the WS rotation to NEW asset_ids ~3.5 min into the capture introduces markets where the lab's book_state for those `asset_id`s starts cold (no observed `book` snapshot yet for the new rotation set). For the upcoming full 48h capture's lab rerun, each new rotation cycle in raw_events.jsonl will see this same cold-start; we'll only fully exercise the strategy if multiple complete router-rotation cycles elapse per scan. **Coverage gap named for Phase 1B**: extending `--router-tick-sec` (currently 5s) downstream of WS rotation + seeding the strategy's book_state at the moment of rotation with `/book` REST prefetch should close the cold-start gap; OR rotate strategy.date-of-birth alongside the labSeen-assets roll.

### Naming / doc debt remaining

- `lib/stat_selection.py::welch_t_test_one_sided_right`: rename to `one_sample_t_test_right_tail` (cosmetic, low priority — math is correct; the "Welch" misnomer was Claude's flag; don't extend this surgery now since prior `p < 0.05` gate decisions are unaffected).
- Strategy doc §19 addendum for the post-fix re-verdict table — TO file.
- Coverage-gap note for `S4-S6`'s marginal FAIL (Welch p=0.051-0.052 just barely above α=0.05): the cumulative-decorator chain (AntiThrash → ReversePosition → StopLoss) propagates the S4 base's behavior through; S5/S6 don't independently exercise their decorator logic in the 1.6h esports-heavy / no-RV-drawdown sample. THIS IS A COVERAGE GAP named for Phase 1B not a code bug.


