# PredictIQ Handoff — 2026-05-23

**Track:** general  
**Created by:** Claude Sonnet 4.6  
**Trigger:** ho — session complete, strategy frozen for forward test  
**Context note:** Session 2026-05-23 complete. Strategy frozen for forward test. See HANDOFF.md for full details.

---

## Status at Handoff

The pipeline is running autonomously on prod. All session changes have been synced via Syncthing. The strategy is **frozen** — no parameter changes until the forward test concludes (~2026-07-18).

---

## What Was Done This Session

### Files Changed

| File | Change | Activation needed? |
|------|--------|--------------------|
| `inference/predict_movements.py` | Expiry gate (30–365d), SERIES_LIMIT 2→3, BRONZE_LATEST constant | Automatic next ETL cycle |
| `inference/exit_evaluator.py` | Bid marking, STOP_LOSS_ROI 10%→20% | Automatic next ETL cycle |
| `api/main.py` | Bid-based P&L, `expires_at` field added | **Needs API process restart** |
| `frontend/src/App.jsx` | × button on all positions, Expires column | **Needs `npm run build`** |
| `~/.claude.json` (local only) | Portainer MCP endpoint 1→3 | Already active locally |

### Key Decisions

- **Stop-loss raised 10% → 20%**: bid-based marking on a typical 6¢-spread / 52¢-entry position reads ~−11.5% at entry, so a 10% SL was firing on brand-new positions that never moved. 20% absorbs the spread; only genuine adverse moves trigger.
- **Expiry gate**: drops short-dated noise (KXTRUMPTIME 7d) and multi-year exotics (KXUSTAKEOVER 1318d). Candidate pool 222 → 159 (−28%).
- **Bid marking everywhere**: dashboard and exit evaluator now show the same number as Kalshi.

---

## Forward Test Plan

| Milestone | Date | Action |
|-----------|------|--------|
| Strategy frozen | 2026-05-23 | No changes from here |
| Early peek | ~2026-06-20 | Check direction only, don't act |
| Definitive check-in | ~2026-07-18 | Measure settled win-rate vs entry-implied probability |

**What to measure:** settled win-rate vs entry-implied probability. NOT mark-to-market P&L.  
**Cohort:** 30–365d expiry entered from 2026-05-23 onward → 38–70d contracts settle Jul/Aug → ~30–50 resolved trades.

---

## Deferred Items (do NOT implement until forward test done)

1. **Environment banner** — `APP_ENV` env var → `/api/config` → frontend title + colored banner
2. **NO-side bias** (~92% briefs) — genuine LLM disposition; reassess after win-rate data
3. **Spread-width entry gate** — not decided
4. **Score-degradation exit** — rejected (entry signal ≠ exit signal)

---

## Resuming This Session

### First things to verify
1. Check API is restarted on prod (or do it): `uvicorn api.main:app --host 0.0.0.0 --port 8000`
2. Check frontend is rebuilt on prod: `cd frontend && npm run build`
3. Confirm ETL is running: Portainer MCP → `list_containers` → `predictiq_spark_etl` should be Up

### Portainer access
- URL: `https://100.67.60.86:9443`
- MCP endpoint ID: **3** (fixed in `~/.claude.json` — local only)

### Key file locations
- Handoff doc (detailed): `HANDOFF.md` (project root)
- Frozen strategy config: see HANDOFF.md § "Frozen strategy config"
- Position ledger: `data/gold/position_ledger.parquet`
- Intelligence briefs: `data/gold/intelligence_briefs/`
- ETL watermark: `data/.etl_watermark`

### Memory files written this session
- `project_forward_test.md` — frozen dates and what to measure
- `project_strategy_tuning_2026_05_23.md` — all file-level changes with rationale
- `project_deferred_items.md` — deferred/rejected items with context

---

## Architecture Snapshot

- **Prod machine:** `xeeeee` (100.67.60.86) — Docker Compose, 13 containers
- **Dev machine:** `n` (100.86.91.43) — Claude Code, API + frontend run directly
- **Sync:** Syncthing over Tailscale (code dev→prod, data prod→dev)
- **Pipeline cadence:** ETL every 5 min, 4 phases (Spark → Embed → Infer → Ledger/Exit)
- **Paper bankroll:** $500, Kelly sizing
