"""
One-off maintenance: dedup the Silver News Delta table.

Why: a watermark bug in silver_news_transform.py compared timestamps as
strings with mismatched separators (bronze 'T' vs silver space), so every
ETL cycle re-appended that day's articles. The table grew to ~2M rows of
duplicates. The bug is now fixed; this script collapses the existing dupes
down to one row per article (keyed on link).

RUN ONCE on prod, with:
  - the ETL container paused (no concurrent writes)
  - the Syncthing data folder paused (so it syncs the final state once)

    python3 transformation/dedup_silver_news.py
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
SILVER_NEWS = os.path.join(ROOT, "data", "silver", "news_articles_enriched")


def run(spark):
    if not os.path.exists(os.path.join(SILVER_NEWS, "_delta_log")):
        print(f"[Dedup] No Delta table at {SILVER_NEWS}. Nothing to do.")
        return

    df = spark.read.format("delta").load(SILVER_NEWS)

    # Schema guard — abort BEFORE any destructive write if the columns the
    # dedup keys on are missing or renamed.
    print("[Dedup] Silver News schema:")
    df.printSchema()
    required = {"link", "ingested_at"}
    missing = required - set(df.columns)
    if missing:
        print(f"[Dedup] ABORT — required column(s) missing: {sorted(missing)}")
        print(f"[Dedup] Table columns are: {df.columns}")
        return

    before = df.count()
    print(f"[Dedup] Rows before: {before:,}")

    # Keep one row per link — the most recent by ingested_at.
    # Rows with a null link can't be keyed, so pass them through untouched.
    has_link = df.filter(F.col("link").isNotNull())
    no_link  = df.filter(F.col("link").isNull())

    w = Window.partitionBy("link").orderBy(F.col("ingested_at").desc_nulls_last())
    deduped = (
        has_link
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .unionByName(no_link, allowMissingColumns=True)
    )

    after = deduped.count()
    print(f"[Dedup] Rows after:  {after:,}  ({before - after:,} duplicates removed)")

    if after == before:
        print("[Dedup] No duplicates found — leaving table untouched.")
        return

    # Overwrite the table with the deduped data.
    (deduped
        .write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(SILVER_NEWS))
    print("[Dedup] Table overwritten.")

    # VACUUM drops the orphaned old parquet files so 2M rows of dead data
    # don't linger on disk and sync over. retain 0 is safe here ONLY because
    # the ETL is paused — nothing else is reading the table.
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    DeltaTable.forPath(spark, SILVER_NEWS).vacuum(0)
    print("[Dedup] VACUUM complete — orphaned files removed.")


def main():
    print("[Dedup] Initializing PySpark session...")
    ivy_dir = os.environ.get("IVY_PACKAGE_DIR", "")
    builder = (
        SparkSession.builder
        .appName("PredictIQ_Dedup_Silver_News")
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
