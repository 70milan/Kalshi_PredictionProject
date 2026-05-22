"""
One-off maintenance: dedup the GDELT Events Silver Delta tables.

Why: a watermark bug in silver_gdelt_events_transform.py compared timestamps as
strings with mismatched separators (bronze 'T' vs silver space), so every ETL
cycle re-appended that batch's events. Both history and current tables accumulated
~8,800 duplicate rows (~0.4%). The bug is now fixed; this script collapses the
existing dupes down to one row per event (keyed on event_id).

RUN ONCE on prod, with:
  - the ETL container paused (no concurrent writes)
  - the Syncthing data folder paused (so it syncs the final state once)

    python3 transformation/dedup_gdelt_events.py
"""
import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

TABLES = [
    ("gdelt_events_history", os.path.join(ROOT, "data", "silver", "gdelt_events_history")),
    ("gdelt_events_current", os.path.join(ROOT, "data", "silver", "gdelt_events_current")),
]


def dedup_table(spark, name, path):
    if not os.path.exists(os.path.join(path, "_delta_log")):
        print(f"\n[{name}] No Delta table at {path}. Nothing to do.")
        return

    df = spark.read.format("delta").load(path)

    print(f"\n[{name}] Schema:")
    df.printSchema()

    if "event_id" not in df.columns:
        print(f"[{name}] ABORT — 'event_id' column missing. Columns: {df.columns}")
        return

    before = df.count()
    print(f"[{name}] Rows before: {before:,}")

    w = Window.partitionBy("event_id").orderBy(F.col("ingested_at").desc_nulls_last())
    deduped = (
        df
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    after = deduped.count()
    print(f"[{name}] Rows after:  {after:,}  ({before - after:,} duplicates removed)")

    if after == before:
        print(f"[{name}] No duplicates found — leaving table untouched.")
        return

    (deduped
        .write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(path))
    print(f"[{name}] Table overwritten.")

    # VACUUM drops orphaned parquet files. retain 0 is safe here ONLY because
    # the ETL is paused — nothing else is reading the table.
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    DeltaTable.forPath(spark, path).vacuum(0)
    print(f"[{name}] VACUUM complete — orphaned files removed.")


def main():
    print("[Dedup GDELT Events] Initializing PySpark session...")
    ivy_dir = os.environ.get("IVY_PACKAGE_DIR", "")
    builder = (
        SparkSession.builder
        .appName("PredictIQ_Dedup_GDELT_Events")
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
        for name, path in TABLES:
            dedup_table(spark, name, path)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
