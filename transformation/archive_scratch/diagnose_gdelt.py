"""
Read-only diagnostic: check the GDELT Silver tables for duplicate records.

Unlike Silver News, the GDELT watermark keeps ISO timestamps on both sides of
the comparison, so its incremental filter should be correct. This script
verifies that before deciding whether any dedup is warranted.

NON-DESTRUCTIVE — it only reads and prints counts. Nothing is written.

    python3 transformation/diagnose_gdelt.py

How to read the output:
  - duplicate rows ~0%        -> watermark is healthy, no dedup needed
  - duplicate rows high, AND
    distinct(id,ts) << total  -> exact re-appends (a watermark bug, like News)
  - duplicate rows high, BUT
    distinct(id,ts) == total  -> same id re-ingested at different times
                                 (could be legit re-processing — do NOT blindly
                                 dedup; decide per table)
"""
import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta import configure_spark_with_delta_pip

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

TABLES = [
    ("gdelt_gkg_history",    "gkg_record_id"),
    ("gdelt_gkg_current",    "gkg_record_id"),
    ("gdelt_events_history", "event_id"),
    ("gdelt_events_current", "event_id"),
]


def run(spark):
    for name, key in TABLES:
        path = os.path.join(ROOT, "data", "silver", name)
        if not os.path.exists(os.path.join(path, "_delta_log")):
            print(f"\n[{name}] no Delta table — skipping")
            continue

        df = spark.read.format("delta").load(path)
        if key not in df.columns:
            print(f"\n[{name}] key column '{key}' missing — columns: {df.columns}")
            continue

        total         = df.count()
        distinct_id   = df.select(key).distinct().count()
        distinct_pair = df.select(key, "ingested_at").distinct().count()
        dup           = total - distinct_id
        pct           = (100.0 * dup / total) if total else 0.0

        print(f"\n[{name}]")
        print(f"  total rows            : {total:,}")
        print(f"  distinct {key:<16s}: {distinct_id:,}")
        print(f"  distinct (id, ts)     : {distinct_pair:,}")
        print(f"  duplicate rows        : {dup:,}  ({pct:.1f}%)")
        if total and distinct_pair < total:
            print(f"  exact re-append rows  : {total - distinct_pair:,}  "
                  f"(same id AND same ingested_at — watermark bug signature)")


def main():
    print("[Diagnose GDELT] Initializing PySpark session...")
    ivy_dir = os.environ.get("IVY_PACKAGE_DIR", "")
    builder = (
        SparkSession.builder
        .appName("PredictIQ_Diagnose_GDELT")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.parquet.enableVectorizedReader", "false")
    )
    if ivy_dir:
        builder = builder.config("spark.jars.ivy", ivy_dir)
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    try:
        run(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
