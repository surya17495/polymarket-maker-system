# Polymarket Maker System

A Polymarket perpetual-futures CLOB market-making **research platform** consisting of two interlocked modes:

- **Live paper-mode capture** — subscribes to Polymarket's public WS market channel, reconstructs limit books, runs simulated maker quotes against the live feed, and emits per-fill Contract 4 ledger rows.
- **Offline Strategy Lab** — replays the SAME captured `raw_events.jsonl` event stream through N strategy variants in seconds, producing per-strategy `ledger_<sid>.parquet` files and a composite-compounding ranking.

The platform exists to answer one question: **What is the most-compounding Polymarket maker strategy, and how does it behave under realistic execution assumptions?** — using empirical measurement rather than model estimation, per the cross-model (GPT-5.5 + Claude) review that anchors the project's strategy doc.

## What justifies this design

Per cross-model review (`docs/strategy_doc.md` rev-2):

- Both GPT and Claude independently recommended: **port `poly_maker/strategy/{quoting, estimators, regime, merge}.py` (the execution infrastructure)**, but **NOT its market-selection logic**. They contested maintainer's self-reported unprofitability (1.2k stars / 436 forks ⇒ generic spread-collection alpha is spent; the *execution* infra is the valuable part).
- The methodology argument: poly-maker is a *market-making bot*; this system is a *research platform that happens to execute trades*. The simulator + comparison + learning loop is the differentiator.
- The empirical-meaning argument: arxiv 2604.24366 (cited by GPT) found trade-direction inferred from the WS book feed agrees with on-chain ground truth only **59–62% of the time**. Therefore the simulator cannot *trust* book-derived fills; it requires an authoritative `/trades` ground truth (`lib/trades_truth.py`).

That ground-truth requirement gates the Phase 1A → Phase 1B promotion (see §8.6 Contract 5 + §8.6 Contract 6 + strategy doc `§8` Phase gates).

## Architecture / data flow

```
       ┌─────────────────────────────────────────────────┐
       │  Polymarket public endpoints                     │
       │   - WS  wss://ws-subscriptions-clob.polymarket.com/ws/market
       │   - REST gamma-api/events /markets (no auth)
       │   - REST clob.polymarket.com/book /prices-history (no auth)
       │   - REST clob.polymarket.com/trades (Phase-2A auth — KYC blocked)
       └─────────────────┬─────────────────────────────────┘
                         │
       ┌─────────────────┴──────────────────────┐
       │                                        │
   LIVE MODE                              LAB MODE (offline replay)
       │                                        │
       ▼                                        ▼
  main_paper.py             lib/strategy_lab.py    ────► run_strategy_lab()
  (see loops/main_paper.py)  (default 7-strategy replay)
       │                                            │
       │ publishes                              reads
       │                                            │
       ▼                                            ▼
  state/raw_events.jsonl  ── (read once) ──►  _run_single_strategy_async
  state/candidates_ranked.parquet                (per-strategy) for sid in S0..S6
       │                                            │
       │                                            ▼
       │                                  MirroredBookStore (lib/mirrored_book.py)
       │                                  auto-derives NO books from YES via
       │                                                  symmetry YES+NO=$1
       │                                            │
       ▼                                            ▼
       │                                  Strategy factory (lib/strategies.py):
       │                                  mounts one of:
       │                                    S0 BBTickStrategy   — baseline
       │                                    S1 PolyQuotingStrategy
       │                                    S2 ReduceOnlyStrategy (= S1 + regime)
       │                                    S3 MergeStrategy (= S2 + MergerState)
       │                                    S4 AntiThrashStrategy (decorator on S3)
       │                                    S5 ReversePositionStrategy (decorator on S4)
       │                                    S6 StopLossStrategy (decorator on S5)
       │                                            │
       │                                            ▼
       │                                  Router (loops/router.py)
       │                                  - calls strategy.quote_at_tick every router_tick_sec
       │                                    of simulated time (router_tick_sec=5s default for lab;
       │                                    60s default for live — both configurable, see config.yaml)
       │                                            │
       │                                            ▼
       │                                  Placement registry (per-asset list of placed
       │                                    QuoteSubmits + queue bounds + arrival book)
       │                                  - emits Contract 4 fill rows when depth-at-our-price
       │                                    shrinks below pre-quote level
       │                                            │
       │                                            ▼
       │                                  After-fills hook (S3+ only):
       │                                    - strategy.after_fill(fill, ts)
       │                                    - MergerState.try_merge_all (S3+):
       │                                       YES+NO join on condition_id → MergeEvent →
       │                                         1−p−q USDC returned to deployable pool
       │                                            │
       ▼                                            ▼
  Backfill markouts (lib/analytics.py):                 │
  backfill_markout_60s_into_ledger (per Raw event)      │
       │                                            │
       │                                            ▼
       └──────────────────────────────────           │
       │                                            ▼
       ▼                                  state/lab/{ledger,merges,summary}_<sid>.parquet
  state/ledger.parquet                  state/lab/ledger_<sid>_validated.parquet
  state/daily_summary.parquet           state/lab/lab_ranking.json
       │                                            │
       └────────────┬───────────────────────────────┘
                    ▼
       lib/trades_truth.py — /trades REST authorship validation
        (Phase-2A auth required; once unblocked, drops the ~40%
         noise floor on book-derived fills per arxiv 2604.24366).
```

## File map

```
polymarket_research/maker_system/
├── api/
│   ├── clob_ws_public.py     — Polymarket WS market channel subscribe/reconnect client
│   ├── clob_rest_public.py   — public REST wrappers (book / prices-history / trades)
│   └── gamma.py              — gamma-api /events /markets; pagination (page_size=100, max_pages=25)
├── lib/
│   ├── book.py               — FIFO per-price-level BookStore by asset_id
│   ├── enriched_score.py     — multi-signal scanner score formula (strategy doc §3)
│   ├── latency_model.py      — WS detect + REST submit latency rolling stats
│   ├── scanner_activity.py   — 15-sec WS activity probe (filter dormant markets)
│   ├── mirrored_book.py      — MirroredBookStore auto-derives NO from YES (symmetry YES+NO=$1)
│   ├── market_pairs.py       — gamma → state/pair_map.parquet (yes/no/condition_id map)
│   ├── poly_estimators.py   — port: time-decayed Ewma + Vol + Flow + MarkoutTracker
│   ├── poly_quoting.py        — port: reservation r=FV−skew; δ=base+c_vol·σ+c_tox·tox;
│   │                            BUY-YES @ r−δ; BUY-NO @ (1−r)−δ; _maybe_exit walk-to-touch by urgency
│   ├── poly_regime.py        — port: 5-state regime machine HALTED>EVENT>REDUCE_ONLY>TRENDING>QUIET
│   ├── poly_merger.py        — MergerState: YES+NO join on condition_id → MergeEvent (1−p−q realized pnl per pair)
│   ├── strategies.py         — S0..S6 catalog + strategy_factory builder (cumulative layering)
│   ├── strategy_lab.py        — offline replay-eval engine → lab_ranking.json
│   ├── trades_truth.py        — /trades REST authorship taker-side ground truth (Phase-2A auth'd, currently 401)
│   ├── compounding_score.py   — composite compounding metric across capital_recycling_rate × (1−as_drag/gross) × (1−tail_rate) × log(fill_count)
│   ├── walk_forward.py       — split raw_events into N train/test windows; walks forward across them
│   └── stat_selection.py     — Welch one-sample right-tailed t-test on per-fill pnl_worst_case
├── loops/
│   ├── router.py             — Strategy ABC + BBTickStrategy(S0) + Router.decide_quote_submits
│   ├── paper_executor.py     — Contract 4 per-fill simulation (queue bounds + stale-state replay; live-async mode)
│   ├── discovery.py          — Loop A scanner (LoopA.run_once_async)
│   ├── analytics.py          — daily_summary aggregator + backfill_markout_60s_into_ledger
│   └── main_paper.py         — phase_1a() driver with periodic re-discovery + WS rotation
├── tests/                    — standalone (no live WS or auth required; __main__ for direct run)
├── config.yaml               — scanner (scan_cycle_sec=300, vol_min=1000, ws_probe_sec=15, ws_probe_top_n=50,
│                                pass_count_top=30), kill_switches (max_quote_lag_ms=30000,
│                                drawdown warn/reduce/halt 1%/2%/3% ladder)
├── dashboard.py              — HTTP server port 8000; `/` HTML auto-refresh + `/api/state` JSON
├── CHANGELOG.md               — see CHANGELOG.md
└── docs/strategy_doc.md      — strategy doc (rev-2, post cross-model review)
```

## Quick start — live paper-mode capture

```bash
cd polymarket_research/maker_system

# Background paper-mode capture for 48 hours (172800s) on top-15 by activity_score:
nohup python3 main_paper.py --phase-1a 172800 --top-n 15 --verbose > state/phase1a_48h.log 2>&1 &

# Dashboard (optional; port 8000, refresh 5s):
python3 dashboard.py &  # http://localhost:8000/ ; /api/state JSON snapshot
```

State artifacts written by the live mode:

| Path | Purpose |
|---|---|
| `state/raw_events.jsonl` | full WS event stream for the active assets (append-only; source for the offline Strategy Lab) |
| `state/candidates_ranked.parquet` | Loop A per-scan set of `pass_count_top=30` ranked markets by activity_score (refreshed every 5 min) |
| `state/ledger.parquet` | per-fill Contract 4 ledger rows (flushed at clean capture end) |
| `state/phase1a_run_summary.json` | rotating-capture live status (heartbeats: scan_count, rotation_count, active_asset_ids, completed_fills_count) |
| `state/latency_summary.json` | rolling p50/p95/p99 latency summaries (WS detect + WS apply + REST book) |
| `state/daily_summary.parquet` | Loop E per-day per-asset aggregate |

## Quick start — Strategy Lab (offline replay)

```bash
cd polymarket_research/maker_system

# Pair-map cache (γ-universe, ~25s, written once):
python3 -c "from lib.market_pairs import build_or_load_pair_map; \
           from pathlib import Path; \
           build_or_load_pair_map(cache_path=Path('state/pair_map.parquet'), refresh=True)"

# Replay lab over the live raw_events.jsonl with all 7 strategies:
python3 lib/strategy_lab.py \
  --raw-events state/raw_events.jsonl \
  --output-dir state/lab \
  --pair-map-cache state/pair_map.parquet \
  --router-tick-sec 5

# Skip the (currently Phase-2A-auth-gated) /trades truth validation:
python3 lib/strategy_lab.py ... --no-trades-truth-validation

# Walk-forward mode: split the capture into 30-min test windows; run lab on each:
python3 lib/strategy_lab.py ... --walk-forward --window-minutes 30
```

Per-strategy artifacts in `state/lab/`:

| Path | Content |
|---|---|
| `state/lab/ledger_<sid>.parquet` | Contract 4 per-fill rows for strategy `sid` |
| `state/lab/ledger_<sid>_validated.parquet` | Subset containing only `/trades`-validated fills; produced when the /trades fetch succeeds (currently 401 — auth pending Phase 2A) |
| `state/lab/merges_<sid>.parquet` | MergeEvents merging YES+NO pair to USDC (S3+) |
| `state/lab/summary_<sid>.parquet` | Per-day per-asset summary |
| `state/lab/trades_<cid>.parquet` | Per-condition_id /trades REST response cache |
| `state/lab/lab_ranking.json` | Sorted strategy ranking (raw + validated metrics) |

## Quick start — tests

The tests deliberately avoid live WS or Phase-2A auth. They run as Python `__main__` scripts to bypass pytest-collection slowness on the WS test:

```bash
for t in tests/test_poly_libs.py \
         tests/test_strategy_lab_smoke.py \
         tests/test_compounding_score.py \
         tests/test_stat_selection.py \
         tests/test_walk_forward.py
do
  python3 "$t" || break
done
```

## Strategy catalog S0..S6

Each strategy implements the same `Strategy.quote_at_tick(book, asset_id, inv, cfg, params, now_ms) -> list[QuoteSubmit]` contract. Each layering **extends the same base** so per-feature attribution is empirically measurable.

| ID | Module | Cumulative improvement |
|---|---|---|
| **S0** | `BBTickStrategy` (in `loops/router.py`) | baseline — BB+tick + inventory-lean, one `QuoteSubmit` per asset per tick |
| **S1** | `PolyQuotingStrategy` | port of `poly_maker/strategy/quoting.py::construct_quotes`. Two-sided BUY-YES @ `r−δ` + BUY-NO @ `(1−r)−δ` where `r = FV − skew`, `δ = base + c_vol·σ + c_tox·toxicity`; post-only placement guards; `_maybe_exit` SELL held inventory walked toward touch by urgency |
| **S2** | `ReduceOnlyStrategy` | + port of `poly_maker/strategy/regime.py::RegimeMachine` — 5-state decision (HALTED > EVENT > REDUCE_ONLY > TRENDING > QUIET); REDUCE_ONLY lets the exit-side `_maybe_exit` continue emitting when the adder is gated by inventory cap |
| **S3** | `MergeStrategy` | + `lib/poly_merger::MergerState` — `after_fill` records per-`condition_id` YES+NO positions; `try_merge_all` emits `MergeEvent` returning `pair_qty × (1 − p − q)` USDC of realized pnl back to the deployable pool (capital recycling mechanism independent of taker-side unwind) |
| **S4** | `AntiThrashStrategy` (decorator on S3) | drop re-quote when `Δmid < 0.10c` and `Δsize < 5%`; reduces sub-tick churn in slow books |
| **S5** | `ReversePositionStrategy` (decorator on S4) | don't BUY the opposing leg when already holding the hedge — avoids doubling into a paired-and-hedged state |
| **S6** | `StopLossStrategy` (decorator on S5) | 3-hr realized-vol-gated stop-loss; 300s post-stop cooldown; take-profit @ 5% above avg cost via increased exit urgency |

## Contract 4 ledger schema

Every per-fill ledger row (whether written by live `paper_executor.py` or by lab `strategy_lab.py`) emits the 32-column schema (post-`/trades`-validation extends with `trades_truth_match_tid`):

```
ts_utc                            uint64 ms
asset_id                          str
market                            str  (condition_id hash)
side_taker                        enum {BUY, SELL}
exec_price                        float
exec_qty                          float (shares)
queue_position_best_case         int  (0 if first-at-price)
queue_position_expected           int
queue_position_worst_case         int  (= depth_at_price / our_size of queue ahead of us)
t_observe_ms                      uint64 (when WS event arrived in our process)
t_arrival_sim_ms                  uint64 (t_observe + sampled latency_ms)
book_hash_at_observe              str  (snapshot hash at observe)
book_hash_at_arrival              str  (post stale-state replay hash at arrival)
mid_observe                       float
mid_at_fill                       float
fair_value_at_fill                float (= mid_at_fill for lab's default FV)
gross_edge_at_fill                float = (exec_price − mid_at_fill) × sign × qty
markout_60s                       float | null
markout_5m                          TODO (extend backfill_60s → 5m + 30m)
markout_30m                         TODO
adverse_selection_drag_60s        float | null
inventory_cost                    float
resolution_cost                   float
fees                              float
rebates                           float
expected_pnl_per_fill             float
pnl_best_case                     float
pnl_expected_case                 float
pnl_worst_case                    float  ← Phase 1A gate signal (Sign-positive + Welch p<0.05)
kill_trigger_fired                str | null
scan_cycle_id                     str
fill_id                           str (uuid)
trades_truth_match_tid            str (extended by lib/strategy_lab post-validation; "" if unmatched)
```

The **Phase 1A gate** is: `Σ pnl_worst_case > 0` across ≥30 fills AND Welch one-sample right-tailed t-test passes at `p < 0.05` (Contract 1 + Contract 3 + §11 measurement-mode).

## Lab ranking output

`state/lab/lab_ranking.json`:

```json
{
  "elapsed_sec": 164.5,
  "ranking": [
    {
      "strategy_id": "s1_poly_quoting",
      "fills": 100,
      "fills_validated_via_trades": 0,        // /trades truth still pending Phase-2A auth
      "quote_submits_total": 116,
      "pnl_worst_sum": 4.20,                  // UPPER BOUND (heuristic book-shrinkage fill detection)
      "pnl_worst_sum_validated": 0.0,         // TRUE LOWER BOUND once /trades truth wired
      "ttest_pass": true,                     // raw — Welch t-test on raw fills
      "ttest_validated_pass": false,          // pending /trades validation
      "composite_score": 0.0,                 // gated because version has not had time to recycle capital
      "composite_score_validated": 0.0,
      "capital_recycling_rate": 0.0,
      "as_drag_per_fill_avg": 0.0,
      "gross_per_fill_avg": <varies>,
      "tail_rate": 0.0,
      "trades_truth_match_rate": 0.0
    },
    {
      "strategy_id": "s0_bb_tick",
      "fills": 32,
      "pnl_worst_sum": 0.0,
      "ttest_pass": false,
      ...
    },
    {"strategy_id": "s2_reduce_only", "fills": 0, ...},
    {"strategy_id": "s3_with_merge", "fills": 0, ...},
    {"strategy_id": "s4_anti_thrash", "fills": 0, ...},
    {"strategy_id": "s5_reverse_pos", "fills": 0, ...},
    {"strategy_id": "s6_stop_loss", "fills": 0, ...}
  ]
}
```

**Sorting rule**: prefer `pnl_worst_sum_validated` (if any fills were validated via `/trades`); otherwise fall back to `pnl_worst_sum` (raw-form lab-heuristic estimate). This means the **lab_ranking is honest** about which signal is qualified.

## How it all works end to end — 4-part pipeline

### Part 1 — Live paper-mode capture (writing `raw_events.jsonl`)

`main_paper.py::phase_1a` boots four concurrent asyncio tasks:

1. **scan_loop** — every `scan_cycle_sec` (default 300s in `config.yaml`):
   - **Loop A** (`loops/discovery.py::LoopA.run_once_async`): gamma paginated fetch (`max_events=2500`); REST `/book` for each candidate market in 20-way parallel; compute `enriched_score`; optional 15-sec WS probe via `lib/scanner_activity::probe_ws_activity` to count price_change msgs per asset and multiply score by `activity_factor`; persist top-`pass_count_top=30` to `state/candidates_ranked.parquet`.
   - `rotate_ws` — if the new top-N candidates differ from current WS-subscribed ones, expire pending paper quotes for dropped assets, pop BookStore entries, stop current WSClient, reconnect with `{"assets_ids": [...], "type": "market"}` payload.
2. **router_tick_loop_state** — every `router_tick_sec` of simulated time (defaults to 60s per `config.yaml::router.tick_sec`):
   - Get the active `seen_assets` list.
   - Call `router.decide_quote_submits(scan_id, list(active_assets), now_ms=t_ms)` → emits `QuoteSubmit` events per asset.
3. **paper_executor_loop** — every 50ms: process pending QuoteSubmits (sample latency, stale-state replay, register placements, detect fills).
4. **latency_heartbeat** — every 15s: write `state/latency_summary.json` (p50/p95/p99 of `ws_detect` and `ws_apply`) and a partial-shape `phase1a_run_summary.json` reflecting live status (the dashboard reads these).

All four tasks run via `asyncio.gather`. On shutdown: `flush.fills_to_parquet()` writes the in-memory `completed_fills` to `state/ledger.parquet`; then `backfill_markout_60s_into_ledger` walks `raw_events.jsonl` to attach `markout_60s` on each fill row; finally `aggregate_daily_summary` writes `state/daily_summary.parquet`.

### Part 2 — Offline Strategy Lab replay

`lib/strategy_lab.py::run_strategy_lab` is the entry point. For each strategy `sid` in `S0..S6`:

1. **Build pair_map** — `_resolve_pair_map_for_asset_ids` (from `state/pair_map.parquet` via `lib/market_pairs.build_or_load_pair_map`); filters map to asset ids in `raw_events` for fast lookup.
2. **Build MirroredBookStore** — wraps a `BookStore` such that every `apply_ws_message(yes_token_book)` writes a parallel NO book by symmetry (`bb_no = 1 − ba_yes`, `ba_no = 1 − bb_yes`).
3. **Build strategy** — `strategy_factory(sid, book_store_inner, pair_map)` returns S0..S6 instance.
4. **Replay loop** — for each event in `raw_events.jsonl` (loaded once into memory):
   - `book_store_mirrored.apply_ws_message(ev)` — applies to YES book + auto-derives NO.
   - Update `seen_assets` set (asset_ids touched + their NO complement from `pair_map`).
   - Periodically (every `router_tick_sec * 1000` ms of simulated time `t_ms`):
     - `quotes = router.decide_quote_submits(scan_id, list(seen_assets), now_ms=t_ms)` — passes `now_ms` to the Router so its liveness check uses simulated time, not real wall-clock (otherwise events hours ahead of now look stale).
     - For each returned `QuoteSubmit`: call `_register_placement` → returns a placement dict with `t_arrival_sim_ms = q.t_observe_ms + 240ms`, queue-position bounds (qpb/qpe/qpw), and clamped "depth_at_arrival" — appended to `placements_by_asset[q.asset_id]`.
   - For each `price_change` event touching asset_id `aid`:
     - Scan `placements_by_asset[aid]` (only placements for THIS asset); for each placement with `t_ms >= t_arrival_sim_ms`:
       - Compute `cur_d = book_now.bids[quote_price]` (BID) or `book_now.asks[quote_price]` (ASK).
       - `shrunk = depth_at_price_at_arrival - cur_d` — depth cleared at our level (positive = someone consumed orders at our price level).
       - If `shrunk > 0`: compute best/exp/worst fill quantities per Contract 1, emit one per-fill Contract 4 row, call `strategy.after_fill(fill, t_ms)` (MergerState learns on S3+; EWMA's MarkoutTracker records the fill's fair-value markout on S1+), pop the placement.
       - If placement has expired (t_ms - t_arrival_sim_ms > quote_timeout_ms), drop it.
5. **After replay**: `_flush_fills_to_parquet_fast` writes `ledger_<sid>.parquet`; `backfill_markout_60s_into_ledger` retroactively fills `markout_60s` per row by walking `raw_events.jsonl`; MergerState emits `merges_<sid>.parquet` (S3+ only).
6. **/trades truth validation** (optional, currently 401): `_validate_fills_via_trades_truth` fetches `/trades?market=<condition_id>&takerOnly=true` for each unique condition_id in the fills, cross-matches each lab fill against authoritative `TradeRecord` (same `asset_id`, same `side_taker`, ts within ±5s), writes `ledger_<sid>_validated.parquet` (subset of only-matched fills), computes `metrics_via_trades_truth` and `ttest_via_trades_truth` (Welch on the validated subset).
7. **Composite compounding score** (per-fill derived): `lib/compounding_score::compute_composite_score` walks the ledger, computes `capital_recycling_rate` (merges + ask-fills / total fills), `as_drag_per_fill_avg` (mean `markout_60s`), `gross_per_fill_avg` (mean `gross_edge_at_fill`), `tail_rate` (fraction of fills where `pnl_worst_case < -1.0`); the composite is tagged per row and persisted on the lab_ranking.json.
8. **Rank** — sort by `pnl_worst_sum_validated` (when present) else `pnl_worst_sum` (raw), and write `state/lab/lab_ranking.json`.

### Part 3 — What each module does (module README)

| Module | Why it exists | Cross-model reference |
|---|---|---|
| `lib/poly_estimators.py` | time-decayed EWMAs feed vol/flow/toxicity into the quoting model | `poly_maker/strategy/estimators.py` (port); natural for regime + quoting dependencies |
| `lib/poly_regime.py` | 5-state per-market gate; REDUCE_ONLY separates "add" from "exit" — the missing piece that the live inventory-saturation pathology surfaced | `poly_maker/strategy/regime.py` (port); named by Claude noting poly-maker "REDUCE_ONLY exits" |
| `lib/poly_quoting.py` | the actual reservation/skew/half-spread formula; constructs both BUY-YES + BUY-NO as the canonical two-sided USDC-collateralized quote | `poly_maker/strategy/quoting.py` (port); modeled exactly |
| `lib/poly_merger.py` | when both YES + NO shares are held, emits a synthetic merge to USDC at locked edge `1-p-q` — capital that does NOT need a taker to take the unwinding ASK | `poly_maker/merge.py` (sim-only, Phase 2A becomes on-chain EIP-712 batch); Claude first named merger |
| `lib/mirrored_book.py` | when the live capture subscribes to YES-only, derive a NO book via `NO best_bid = 1 − YES best_ask`, `NO best_ask = 1 − YES best_bid`, sizes mirrored — exact for Polymarket binary markets | not in poly-maker (they sub both sides); lab-only innovation |
| `lib/market_pairs.py` | gamma → `state/pair_map.parquet` of `asset_id → (yes_token, no_token, condition_id)`. Caches once (~30k markets) | lab-only innovation |
| `lib/strategies.py` | the catalog S0..S6 `strategy_factory` builder; cumulative layering makes each port-list feature individually attributable | bridges poly_maker to our strategy API |
| `lib/strategy_lab.py` | offline replay engine + lab_ranking writer | lab-only innovation |
| `lib/trades_truth.py` | `/trades` authorship-fetcher (paginated) + per-future per-window signed-taker-flow aggregator | GPT explicit (arxiv 2604.24366) |
| `lib/compounding_score.py` | composite compounding formula | born from cross-model "research platform" framing |
| `lib/walk_forward.py` | window split + walk-forward pair generator | born from strategy-doc § 11 Phase 1B gate discipline |
| `lib/stat_selection.py` | Welch one-sample right-tailed t-test; falls back to scipy-free math approximation | born from GPT § 8.6 Contract 5 Phase 1B gate |

### Part 4 — Interpretation of the empirical finding

Live 41k-msg raw_events replayed over the 7-strategy catalog (`state/lab/lab_ranking.json`, dated 2026-07-24 22:55 UTC):

- `s1_poly_quoting` (port of poly-maker quote model + two-sided quoting) — **100 fills, Σ `pnl_worst_case = +$4.20` USD, Welch one-sample right-tailed t-test PASSES at p≈0 (n=100, mean=0.042, t=15.96)**. **First Phase 1A signal** — the strategy clears the worst-case queue bound + statistical edge threshold. **UPPER BOUND until `/trades` truth validates which simulated fills actually matched an authoritative /trades entry** — currently 401 until Phase 2A KYC.

- `s0_bb_tick` (baseline) — 32 fills, `pnl_worst_case = $0.00` for every row (queue_ahead_others_shares consumed all the depth so the worst-case fill_qty is always 0); Welch FAILS. The original `BBTickStrategy` has no edge conclusion.

- `s2..s6` — these are down-in-the-cumulative-catalog; in the live replay they produced many quote submissions but zero fills because the regime machine's `halt_before_hours=1.0` default HALTs soon-to-resolve esports matches, and the antithrash deployament is gated at `Δmid<0.5c` — the threshold for stable markets where poly-quoting legitimately places the same quote price repeatedly. (Both values were lowered after this run to `halt_before_hours=0.25` and `Δmid<0.10c`; the live replay wasn't re-run yet — see `CHANGELOG.md` WIP next-session queue.)

`§11 Predicted returns — PENDING EMPIRICAL MEASUREMENT` null anchors: Sonnett tail-weighted $1–$6/day (low) vs Centri optimistic $50–$300/day (high). The empirical S1 `+$4.21` extrapolates to ≈$63/day over 24h — sits between the anchors. NOT yet committed to §11 — requires either (a) `/trades` truth validation wiring through Phase-2A credentials and confirm 89-100% of those 100 fills were authoritatively real; or (b) a true 7-14 day Phase 1B regime-validation capture.

## Cross-model review summary (verbatim condensed)

| Architecture decision | Source |
|---|---|
| Per-fill Contract 4 ledger (queue bounds + per-fill PnL accounting, not pool-share) | GPT §8.6 (defects a, b, c) |
| Phase 1A gate = positive net PnL ≥0 under worst-case queue bounds + Welch p<0.05 | GPT (cross-model review reframing) |
| Phase 1A = 48h ops; Phase 1B = 7-14d regime; Phase 1B→2A promotion requires all 7-day gate metrics | GPT, Claude (converge) |
| Drawdown ladder 1%/2%/3% (warn/reduce/halt) replacing 8% breaker | GPT (post-cross-model tightening) |
| Leverage (Phase 2C stepwise unlock: 1.25× → 14d → 1.5× → 14d → 2×) deferred behind 30-day positive-pnl gate | GPT + Claude (second independent reason) |
| Port-list: poly_maker/strategy/quoting.py + estimators.py + regime.py + merge.py + post-only + anti-thrash + reverse-position + EWMA | GPT + Claude (converge on identical list) |
| Do NOT port poly_maker's market-selection (manual Google Sheet) | GPT + Claude (converge) |
| `position merging` `poly_merger` capitol-efficiency unlock (capital-recycling without taker-side unwind) | Claude first to flag (GPT later elaborated) |
| arxiv 2604.24366 — book-derived trade direction only 59-62% reliable vs on-chain ground truth ⇒ `/trades` truth is mandatory for AS regressor ground truth | GPT (only GPT flagged) |
| `anti-thrash` Δmid>0.5c OR Δsize>10% threshold generously greedy threshold | Claude (alone) — confirmed by Qualcomm-Welch-open source grep of poly_maker |
| `reverse-position check` (don't buy opposing leg when already hedged) | Claude + GPT |
| Strategy doc §11 estimates-wander audit — 9 numbers quoted across ~20 turns | Centri (self-critique) |
| Both Sonnett + Centri anchor values become NULL HYPOTHESES holding steady on `/trades` validation | GPT (open-ended gate outcome framing) |

## Status (as of this commit)

- **Phase 0 build**: COMPLETE (per `docs/strategy_doc.md §17`).
- **Phase 1A build**: COMPLETE (per §18 + §19) — 9/9 unit tests pass; live capture (PID 140060) operational; first empirical Phase 1A signal value uncommitted (`+$4.21 S1 pnl_worst_sum` upper-bound).
- **Phase 1B build**: scaffolded. `lib/walk_forward.py` + `lib/stat_selection.py` ready; `/trades` truth infrastructure built — blocked behind Phase-2A auth; pending Phase-2A KYC unlock to complete Contract 5 (AS regressor ground truth) + Contract 6 (information-flow classifier).
- **Phase 2A build**: pending — needs Polymarket Polygon wallet + L2 REST auth integration; current code paths are all-auth-free.

## License

Private project. Internal build for research metrics; not deployed with real capital.
