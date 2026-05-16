#!/usr/bin/env python3
"""
PredictIQ ETL Orchestrator (run_etl.py)
=======================================
Single-container sequential controller for all Silver and Gold
transformations. Guarantees at most ONE PySpark JVM is active at
any time, capping memory at spark.driver.memory = 4 GB.

Execution Pattern (every 5 minutes):
    1. Run Kalshi daily settlement (once per 24 h)
    2. Check Bronze watermarks for new data
    3. Run 4 Silver transforms (sequential)
    4. Run 3 Gold transforms  (sequential)
    5. Sleep 300 s
"""

import os
import sys
import time
import subprocess
import traceback
from datetime import datetime, timezone

# ─────────────────────────────────────────────
# PATHS
# ─────────────────────────────────────────────

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

WATERMARK_FILE       = os.path.join(PROJECT_ROOT, "data", ".etl_watermark")
DAILY_MARKER_FILE    = os.path.join(PROJECT_ROOT, "data", ".daily_settlement_marker")

BRONZE_PATHS = [
    os.path.join(PROJECT_ROOT, "data", "bronze", "kalshi_markets", "open"),
    os.path.join(PROJECT_ROOT, "data", "bronze", "bbc"),
    os.path.join(PROJECT_ROOT, "data", "bronze", "cnn"),
    os.path.join(PROJECT_ROOT, "data", "bronze", "foxnews"),
    os.path.join(PROJECT_ROOT, "data", "bronze", "nyt"),
    os.path.join(PROJECT_ROOT, "data", "bronze", "hindu"),
    os.path.join(PROJECT_ROOT, "data", "bronze", "nypost"),
    os.path.join(PROJECT_ROOT, "data", "bronze", "gdelt", "gdelt_events"),
    os.path.join(PROJECT_ROOT, "data", "bronze", "gdelt", "gdelt_gkg"),
]

# ─────────────────────────────────────────────
# SCRIPT MANIFESTS (execution order matters)
# ─────────────────────────────────────────────

DAILY_SETTLEMENT_SCRIPT = os.path.join(
    PROJECT_ROOT, "ingestion", "kalshi_daily_settlement.py"
)

# All 7 Silver+Gold transforms now run in a single SparkSession via spark_pipeline.py.
# One JVM startup instead of 7 (~14 minutes saved per cycle).
SPARK_PIPELINE_SCRIPT = os.path.join(PROJECT_ROOT, "orchestration", "spark_pipeline.py")

# Phase 3: Vector Bridge (Delta -> ChromaDB)
VECTOR_SYNC_SCRIPT = os.path.join(PROJECT_ROOT, "rag", "embed_silver_data.py")

# Phase 4: Inference (LLM Synthesis - Predictive Scanner)
INFERENCE_SCRIPT = os.path.join(PROJECT_ROOT, "inference", "predict_movements.py")

# Phase 4b: Position tracking + exit evaluation
LEDGER_SCRIPT = os.path.join(PROJECT_ROOT, "inference", "build_position_ledger.py")
EXIT_SCRIPT   = os.path.join(PROJECT_ROOT, "inference", "exit_evaluator.py")

POLL_INTERVAL = 300   # 5 minutes AFTER the cycle completes (not a fixed cadence)


# ─────────────────────────────────────────────
# WATERMARK HELPERS
# ─────────────────────────────────────────────

def get_watermark():
    """Returns the Unix timestamp of the last successful ETL run."""
    if os.path.exists(WATERMARK_FILE):
        return os.path.getmtime(WATERMARK_FILE)
    return 0


def set_watermark():
    """Touches the watermark file to record the current time."""
    os.makedirs(os.path.dirname(WATERMARK_FILE), exist_ok=True)
    with open(WATERMARK_FILE, "w") as f:
        f.write(datetime.now(timezone.utc).isoformat())


def has_new_bronze_data(watermark_ts):
    """Check if any Bronze file has been modified since the last ETL run."""
    for bronze_dir in BRONZE_PATHS:
        if not os.path.exists(bronze_dir):
            continue
        for root, _dirs, files in os.walk(bronze_dir):
            for fname in files:
                if fname.startswith(".") or fname.startswith("_"):
                    continue
                filepath = os.path.join(root, fname)
                try:
                    if os.path.getmtime(filepath) > watermark_ts:
                        return True
                except OSError:
                    continue
    return False


# ─────────────────────────────────────────────
# DAILY SETTLEMENT GATE
# ─────────────────────────────────────────────

def should_run_daily_settlement():
    """Returns True if the daily settlement has not yet run today (UTC)."""
    if not os.path.exists(DAILY_MARKER_FILE):
        return True
    marker_date = datetime.fromtimestamp(
        os.path.getmtime(DAILY_MARKER_FILE), tz=timezone.utc
    ).date()
    return datetime.now(timezone.utc).date() > marker_date


def mark_daily_settlement_done():
    """Touches the daily marker file."""
    os.makedirs(os.path.dirname(DAILY_MARKER_FILE), exist_ok=True)
    with open(DAILY_MARKER_FILE, "w") as f:
        f.write(datetime.now(timezone.utc).isoformat())


# ─────────────────────────────────────────────
# SUBPROCESS RUNNER
# ─────────────────────────────────────────────

def run_script(script_path, timeout_seconds=600):
    """
    Runs a Python script as a child process.
    Returns True on success (exit 0), False otherwise.
    Only one child process exists at any time (sequential guarantee).
    """
    script_name = os.path.basename(script_path)
    print(f"    > {script_name} ... ", end="", flush=True)
    start = time.time()

    try:
        result = subprocess.run(
            [sys.executable, "-u", script_path],
            cwd=PROJECT_ROOT,
            timeout=timeout_seconds,
        )
        elapsed = time.time() - start

        if result.returncode == 0:
            print(f"OK ({elapsed:.1f}s)")
            return True
        else:
            print(f"FAILED (exit {result.returncode}, {elapsed:.1f}s)")
            return False

    except subprocess.TimeoutExpired:
        print(f"TIMEOUT ({timeout_seconds}s)")
        return False
    except Exception as e:
        print(f"ERROR: {e}")
        return False


# Lock file written by the active ingestor while it's making API calls.
KALSHI_LOCK_FILE = os.path.join(PROJECT_ROOT, "data", ".kalshi_api.lock")


def is_kalshi_api_busy():
    """Returns True if the active ingestor is currently making API calls."""
    if not os.path.exists(KALSHI_LOCK_FILE):
        return False
    # If lock file is older than 30 minutes, consider it stale (ingestor crashed)
    try:
        age = time.time() - os.path.getmtime(KALSHI_LOCK_FILE)
        return age < 1800  # 30 minutes
    except OSError:
        return False


def run_etl_cycle():
    """Execute one full ETL cycle: Silver -> Gold -> Settlement (last)."""
    cycle_start_time = time.time()
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"\n{'='*60}")
    print(f" PredictIQ ETL Orchestrator")
    print(f" Cycle Start: {now}")
    print(f"{'='*60}")

    # --- Phase 1: Check for new Bronze data -> Silver -> Gold ---
    watermark = get_watermark()
    if not has_new_bronze_data(watermark):
        print("\n[Phase 1/3] No new Bronze data detected. Skipping transforms.")
    else:
        # --- Silver + Gold in a single Spark session ---
        # spark_pipeline.py runs all 7 transforms inside one JVM. Exit code 0
        # means everything succeeded; 1 means at least one transform failed
        # (the script keeps going past per-transform failures).
        print("\n[Phase 1/3] Silver + Gold Transformations (unified Spark session)")
        print("-" * 40)
        # Generous timeout: cold start + all 7 transforms. Previous separate-script
        # timeouts summed to ~4 hours; ~50 min is plenty for one shared session.
        pipeline_ok = run_script(SPARK_PIPELINE_SCRIPT, timeout_seconds=3000)

        # --- Update watermark ---
        set_watermark()

        print(f"\n    [ RESULT ] Spark pipeline {'succeeded' if pipeline_ok else 'had partial failures'}.")

        # --- Phase 2: Vector Sync ---
        print("\n[Phase 2/3] Vector Bridge Synchronization")
        print("-" * 40)
        run_script(VECTOR_SYNC_SCRIPT, timeout_seconds=2400)

        # --- Phase 3: Inference Engine ---
        print("\n[Phase 3/3] AI Inference & Mispricing Detection")
        print("-" * 40)
        run_script(INFERENCE_SCRIPT, timeout_seconds=1800)  # 30 min for large market batches

    # --- Phase 4b: Position Ledger + Exit Evaluation (runs EVERY cycle, not gated on bronze)
    # Exits are time-sensitive — prices move regardless of new bronze data.
    print("\n[Phase 4b] Position Ledger + Exit Evaluation")
    print("-" * 40)
    run_script(LEDGER_SCRIPT, timeout_seconds=120)
    run_script(EXIT_SCRIPT,   timeout_seconds=120)

    # --- Phase 5: Daily Settlement (once per day, AFTER transforms) ---
    if should_run_daily_settlement():
        if is_kalshi_api_busy():
            print("\n[Phase 5/5] Daily Settlement -- Active ingestor is running, deferring.")
        else:
            print("\n[Phase 5/5] Kalshi Daily Settlement Sweep")
            print("-" * 40)
            if run_script(DAILY_SETTLEMENT_SCRIPT, timeout_seconds=3600):
                mark_daily_settlement_done()
            else:
                print("    Settlement failed. Will retry next cycle.")
    else:
        print("\n[Phase 5/5] Daily Settlement -- already ran today, skipping.")

    cycle_duration = time.time() - cycle_start_time
    mins = int(cycle_duration // 60)
    secs = int(cycle_duration % 60)
    print(f"\n[ETL] Cycle completed in {mins}m {secs}s")
    print(f"{'='*60}\n")


# ─────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────

def main():
    print("=" * 60)
    print(" PredictIQ ETL Orchestrator v1.0")
    print(" Sequential Batch Controller (5-min polling)")
    print("=" * 60)

    while True:
        try:
            run_etl_cycle()
        except Exception:
            print("[ETL ORCHESTRATOR] CRITICAL ERROR:")
            traceback.print_exc()

        print(f"[ETL] Sleeping {POLL_INTERVAL}s...")
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
