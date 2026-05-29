# PredictIQ — Session Handoff (2026-05-23)

Context: tuning the inference/exit logic, fixing dashboard pricing, and **freezing the
strategy for an 8-week forward test**. Pipeline runs autonomously on prod (Docker); it keeps
trading/logging without anyone watching. Next human check-in: **~2026-07-18**.

---

## TL;DR for resuming

1. The strategy is **frozen as of 2026-05-23**. Do NOT change parameters mid-test — every
   change resets the experiment. Let a cohort run to settlement.
2. To evaluate (on return): measure **settled (resolved) win-rate vs entry-implied
   probability** — NOT mark-to-market P&L on open positions (that's spread/wiggle noise).
3. All exit triggers (TP/SL/backstops/time-decay/thesis-flip) are **recommendations only** —
   nothing auto-sells. Positions close only via the dashboard × button or market settlement.

---

## Changes made this session (all in local files; review before trusting prod)

### `inference/predict_movements.py`
- **Added contract-expiry gate**: candidates must have **30–365 days to close**
  (env: `INFERENCE_MIN_DAYS_TO_CLOSE` / `INFERENCE_MAX_DAYS_TO_CLOSE`). Implemented by
  joining the gold ledger to bronze `latest.parquet` (only source of `close_time`).
  Validated live: candidate pool 222 → 159 (~28% trimmed). Drops short-dated noise
  (KXTRUMPTIME 7d) and multi-year contracts (KXUSTAKEOVER 1318d, KXAFRICALEADEROUT 3145d).
- **`SERIES_LIMIT` 2 → 3** (max candidates per market series per cycle).
- Added `BRONZE_LATEST` path constant.

### `inference/exit_evaluator.py`
- **Marking reverted to bid** (was mid). Dashboard now matches what Kalshi shows.
- **`STOP_LOSS_ROI` 10% → 20%** — this is the load-bearing change: bid-based marking on a
  ~6¢-spread / 52¢-entry position is already ~-11.5% at entry, which tripped a 10% stop on
  positions that never moved. 20% absorbs the typical spread so only genuine 20%+ adverse
  moves trigger. (TAKE_PROFIT_ROI unchanged at 65%.)

### `api/main.py`
- `/api/paper/positions` and `/api/exits`: **reverted to bid-based** current_price/P&L
  (consistent with exit_evaluator). `suggested_exit_price` already was the bid.
- `/api/paper/positions`: **added `expires_at`** (contract close date) by pulling `close_time`
  from bronze `latest.parquet` into the price map.

### `frontend/src/App.jsx`
- **Close (×) button now shows on ALL open positions** (previously only SL_HIT / TP_READY).
  Hidden only for CLOSED / SETTLED rows.
- **Added "Expires" column** to the Open Positions tab (after "Entry") — sortable/filterable,
  shows each contract's resolution date.

### `~/.claude.json` (local Claude config, NOT in repo, does NOT sync)
- Fixed Portainer MCP endpoint **1 → 3** for the `C:\Users\milan` scope. Container control
  via the portainer MCP now works (only environment is endpoint ID=3).

---

## Deployment status — what's live vs what needs action

| File | Syncs dev→prod? | Activation |
|---|---|---|
| `predict_movements.py` | yes | **Automatic** next ETL cycle (run_etl spawns it fresh each run) |
| `exit_evaluator.py` | yes | **Automatic** next ETL cycle |
| `api/main.py` | yes | **Needs API process restart** (no auto-reload unless `uvicorn --reload`) |
| `frontend/src/App.jsx` | yes | **Needs `cd frontend && npm run build`** (or hot-reload if `npm run dev`) |

The API runs **locally** (no API container exists in Portainer) on whichever machine serves
the dashboard. Three instances run: dev (`100.86.91.43:8000`), read-only
(`READONLY_MODE=true`, `127.0.0.1:8001`), and prod.

---

## Frozen strategy config (baseline 2026-05-23 — for drift detection)

**Entry** (`predict_movements.py`): mispricing_score ≥ 80 · yes_bid 0.25–0.75 · expiry
30–365d · freshness ≤48h · confidence ≥65% · edge ≥+10pp · RAG ≥0.63 · Kelly positive ·
SERIES_LIMIT=3 · CANDIDATE_LIMIT=50 · budget $1/day · gpt-4o-mini. Re-eligibility: re-scan
only if price moved ≥5pp OR news re-spiked ≥1.5× (floor 3×), 15-min anti-thrash, 24h ceiling.

**Exit** (`exit_evaluator.py`): bid marking · TP +65% ROI · SL −20% ROI · backstops
80%-of-max-gain & 20¢ absolute · time-decay <24h±5¢ · thesis-flip. Sizing: $500 paper
bankroll, Kelly.

---

## Forward-test plan

- **Frozen:** 2026-05-23. Reason: clean cohort needed; parameter churn invalidates the test.
- **Definitive check-in:** ~2026-07-18 (8 weeks) — the 38–70 day expiry cohort (Jul 1 / Aug 1)
  will have settled → ~30–50 resolved trades = real sample.
- **Optional early peek:** ~2026-06-20 (4 weeks) — thin, directional only, don't act on it.
- **What to measure:** settled win-rate vs entry-implied probability. Ignore mark-to-market.
- No downside protection while away (SL is recommendation-only) — a position can ride to a
  losing settlement. Fine for a $500 paper test of *entry* edge.

---

## Open / deferred items (NOT implemented — decide later)

- **Environment banner + browser-tab title** (dev / prod / read-only). Discussed approach:
  one `APP_ENV` env var per launch → surfaced via existing `/api/config` → frontend sets
  `document.title` + a colored top banner (green=dev, red=prod, gray=read-only). Label must
  come from the API env var (NOT build-time) because read-only and prod share the same build.
- **NO-side bias** (~92% of briefs recommend NO). Diagnosed as genuine LLM disposition on
  longshot markets, NOT a parse bug. Deferred: judge by win-rate after the forward test
  before any prompt rebalancing.
- **Spread-width entry gate** — idea floated (skip trades where spread is a large % of max
  gain). Not decided.
- **Score-degradation exit trigger** — considered and **rejected** (mispricing score is an
  entry signal, not an exit signal; a faded news spike ≠ broken thesis).

## Known cosmetic (non-breaking) items
- `gold_market_summaries_transform.py` line ~283 comment says "Flag candidates > 80" but code
  flags at `> 65.0`. `flagged_candidate` is unused by inference (it filters on score ≥ 80
  directly), so harmless.
- `get_predictive_candidates(min_score=80.0)` param is decorative — SQL hardcodes `>= 80`.

---

## Deploy topology reminder
Syncthing: **CODE dev→prod only**, **DATA prod→dev only**. No git in repo. To verify prod
data, read the local `data/` mirror. Portainer = endpoint ID=3.
