#!/usr/bin/env python3
"""
PredictIQ Spark Pipeline (spark_pipeline.py)
============================================
Runs all 7 PySpark transforms (4 Silver + 3 Gold) inside a single
SparkSession to amortize JVM startup cost. Replaces what used to be
7 separate subprocess calls — saves ~14 minutes of JVM warmup per
ETL cycle.

Order matters: silver → gold (gold depends on silver outputs).
A failure in one transform is logged but does not kill the pipeline;
the next transform still runs.
"""

import os
import sys
import time
import traceback

# Bootstrap path so we can import transform modules
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

# Force Python interpreter for PySpark on Windows/Linux parity
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

from transformation import silver_kalshi_transform
from transformation import silver_news_transform
from transformation import silver_gdelt_events_transform
from transformation import silver_gdelt_gkg_transform
from transformation import gold_market_summaries_transform
from transformation import gold_news_summaries_transform
from transformation import gold_gdelt_summaries_transform


# ─────────────────────────────────────────────
# UNIFIED SPARK CONFIG
# ─────────────────────────────────────────────
# Combines every config flag any of the 7 transforms needed:
#   - schema.autoMerge (Kalshi silver MERGE)
#   - arrow.pyspark.enabled=false (news silver collect-to-pandas stability)
#   - shuffle.partitions=16 + AQE (gold window/join scripts)
#   - parquet vectorized reader off (Delta on GDELT)
def build_spark():
    print("[Pipeline] Initializing shared PySpark Session...")
    ivy_dir = os.environ.get("IVY_PACKAGE_DIR", "")
    builder = (
        SparkSession.builder
        .appName("PredictIQ_Unified_ETL")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.minPartitionSize", "32mb")
        .config("spark.sql.files.maxPartitionBytes", "128mb")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .config("spark.sql.parquet.enableVectorizedReader", "false")
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
    )
    if ivy_dir:
        builder = builder.config("spark.jars.ivy", ivy_dir)

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


# ─────────────────────────────────────────────
# TRANSFORM REGISTRY
# ─────────────────────────────────────────────
# Order is load-bearing: silver feeds gold, GDELT silver feeds GDELT gold.

SILVER_TRANSFORMS = [
    ("Silver Kalshi",       silver_kalshi_transform.run),
    ("Silver News",         silver_news_transform.run),
    ("Silver GDELT Events", silver_gdelt_events_transform.run),
    ("Silver GDELT GKG",    silver_gdelt_gkg_transform.run),
]

GOLD_TRANSFORMS = [
    ("Gold News Summaries",   gold_news_summaries_transform.run),
    ("Gold GDELT Summaries",  gold_gdelt_summaries_transform.run),
    ("Gold Market Summaries", gold_market_summaries_transform.run),  # depends on the other two golds
]


def run_one(spark, label, fn):
    """Runs one transform. Returns True on success, False on failure (does not raise)."""
    print(f"\n[Pipeline] ▶ {label}")
    print("-" * 50)
    start = time.time()
    try:
        fn(spark)
        elapsed = time.time() - start
        print(f"[Pipeline] ✓ {label} OK ({elapsed:.1f}s)")
        return True
    except Exception:
        elapsed = time.time() - start
        print(f"[Pipeline] ✗ {label} FAILED ({elapsed:.1f}s)")
        traceback.print_exc()
        # Drop any cached state the failing transform left behind so it can't
        # leak into the next transform.
        try:
            spark.catalog.clearCache()
        except Exception:
            pass
        return False


def main():
    overall_start = time.time()
    print("=" * 60)
    print(" PredictIQ Unified Spark Pipeline")
    print("=" * 60)

    spark = build_spark()
    results = []  # list of (label, ok)

    try:
        print("\n[Pipeline] === SILVER LAYER ===")
        for label, fn in SILVER_TRANSFORMS:
            results.append((label, run_one(spark, label, fn)))

        print("\n[Pipeline] === GOLD LAYER ===")
        for label, fn in GOLD_TRANSFORMS:
            results.append((label, run_one(spark, label, fn)))
    finally:
        spark.stop()

    total_elapsed = time.time() - overall_start
    succeeded = sum(1 for _, ok in results if ok)
    total = len(results)

    print("\n" + "=" * 60)
    print(f" Pipeline complete: {succeeded}/{total} transforms succeeded in {total_elapsed:.1f}s")
    print("=" * 60)
    for label, ok in results:
        mark = "✓" if ok else "✗"
        print(f"  {mark} {label}")

    # Exit non-zero if any transform failed — orchestrator uses this to decide
    # whether to surface a partial-failure message.
    sys.exit(0 if succeeded == total else 1)


if __name__ == "__main__":
    main()
