import os
import sys
import fnmatch
import json
import math
import pandas as pd
import numpy as np

# Force PySpark workers to use the exact virtual environment Python executable
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# Windows compatibility: ensure HADOOP_HOME bin is on PATH if env var is set
_hadoop_home = os.environ.get("HADOOP_HOME", "")
if _hadoop_home:
    os.environ["PATH"] = os.path.join(_hadoop_home, "bin") + ";" + os.environ.get("PATH", "")

# Fix Windows PySpark BlockManager NullPointerException (Heartbeat loop)
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, TimestampType
from delta import configure_spark_with_delta_pip
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# ─────────────────────────────────────────────
# WATERMARK & INCREMENTAL READ UTILITIES
# ─────────────────────────────────────────────

def get_watermark(spark, silver_path):
    """Returns MAX ingested_at from Silver Delta table, or None on cold start."""
    delta_log = os.path.join(silver_path, "_delta_log")
    if os.path.exists(delta_log):
        try:
            return spark.read.format("delta").load(silver_path) \
                .select(F.max("ingested_at")).collect()[0][0]
        except Exception:
            return None
    return None


def read_bronze_incremental(spark, bronze_files, watermark):
    """Read Bronze Parquet files, filter to rows newer than watermark."""
    if not bronze_files:
        raise ValueError("[Silver News] No Bronze Parquet files found.")
    df = spark.read.option("mergeSchema", "true").parquet(*bronze_files)
    if watermark is not None:
        df = df.filter(F.col("ingested_at") > str(watermark))
    return df


def write_history(spark, df, silver_path):
    """Append enriched rows to Silver Delta table with idempotency guard."""
    delta_log = os.path.join(silver_path, "_delta_log")
    if os.path.exists(delta_log):
        try:
            batch_min = df.select(F.min("ingested_at")).collect()[0][0]
            already = spark.read.format("delta").load(silver_path) \
                .filter(F.col("ingested_at") >= batch_min).limit(1).count() > 0
            if already:
                print("[Silver News] Batch already exists - skipping append.")
                return
        except Exception:
            pass
    df.write.format("delta").mode("append").option("mergeSchema", "true").save(silver_path)


# ─────────────────────────────────────────────
# TRANSFORM (Driver-based, Zero-Worker Write)
#
# ARCHITECTURE: "JSON Bridge" pattern
# On this Windows environment, ALL PySpark Python worker processes crash.
# pyarrow import also hangs. So we:
# 1. Read Bronze with Spark (pure JVM read of Parquet — stable)
# 2. Collect to Pandas via toPandas() (JVM collectToPython — stable)
# 3. Enrich entirely in Pandas on the driver (pure Python — stable)
# 4. Save enriched data as temp JSON using built-in json (no pyarrow needed)
# 5. Read the temp JSON back with Spark (pure JVM read — stable)
# 6. Cast timestamp columns and write to Delta (pure JVM — stable)
# Result: Python workers are NEVER invoked.
# ─────────────────────────────────────────────

def enrich_on_driver(pdf):
    """Enrich a Pandas DataFrame with VADER sentiment. Pure Python, no Spark."""
    if len(pdf) == 0:
        return pdf

    # 1. Normalize published_at from various source column names
    if "published_at" not in pdf.columns:
        pdf["published_at"] = None

    if "pubDate" in pdf.columns:
        pdf["published_at"] = pdf["published_at"].fillna(pdf["pubDate"])
        pdf.drop(columns=["pubDate"], inplace=True)

    if "published" in pdf.columns:
        pdf["published_at"] = pdf["published_at"].fillna(pdf["published"])
        pdf.drop(columns=["published"], inplace=True)

    # 2. Robust date parsing then convert to ISO string for JSON serialization
    pdf["published_at"] = pd.to_datetime(pdf["published_at"], errors="coerce", utc=True)
    pdf["ingested_at"] = pd.to_datetime(pdf["ingested_at"], errors="coerce", utc=True)

    # Convert to ISO strings (Spark reads these perfectly as timestamps)
    pdf["published_at"] = pdf["published_at"].apply(
        lambda x: x.strftime("%Y-%m-%dT%H:%M:%S") if pd.notna(x) else None
    )
    pdf["ingested_at"] = pdf["ingested_at"].apply(
        lambda x: x.strftime("%Y-%m-%dT%H:%M:%S") if pd.notna(x) else None
    )

    # 3. Ensure text columns exist
    for col in ["summary", "full_text", "title"]:
        if col not in pdf.columns:
            pdf[col] = ""
        else:
            pdf[col] = pdf[col].fillna("").astype(str)

    # 4. Select richest text for sentiment
    def pick_text(row):
        if len(row["full_text"].strip()) > 0:
            return row["full_text"], "full_text"
        if len(row["summary"].strip()) > 0:
            return row["summary"], "summary"
        return row["title"], "title"

    results = pdf.apply(pick_text, axis=1)
    pdf["_sent_text"] = [r[0] for r in results]
    pdf["sentiment_source"] = [r[1] for r in results]

    # 5. VADER sentiment
    print(f"[Silver News] Running VADER on {len(pdf)} rows...")
    analyzer = SentimentIntensityAnalyzer()

    def score(text):
        if not text or not text.strip():
            return 0.0
        return float(analyzer.polarity_scores(text)["compound"])

    pdf["sentiment_score"] = pdf["_sent_text"].apply(score)
    pdf["sentiment_label"] = pdf["sentiment_score"].apply(
        lambda s: "Positive" if s >= 0.05 else ("Negative" if s <= -0.05 else "Neutral")
    )

    pdf.drop(columns=["_sent_text"], inplace=True)
    return pdf


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def main():
    print("[Silver News] Initializing PySpark Session...")

    ivy_dir = os.environ.get("IVY_PACKAGE_DIR", "")
    builder = (
        SparkSession.builder
        .appName("PredictIQ_Silver_News")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.parquet.enableVectorizedReader", "false")
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
    )
    if ivy_dir:
        builder = builder.config("spark.jars.ivy", ivy_dir)

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # Discover Bronze files
    news_sources = ["bbc", "cnn", "foxnews", "nypost", "nyt", "thehindu"]
    bronze_files = []
    for source in news_sources:
        source_dir = os.path.join(ROOT, "data", "bronze", source)
        if os.path.isdir(source_dir):
            for r, d, f in os.walk(source_dir):
                # Filter *.parquet but EXCLUDE latest.parquet to avoid duplication
                for filename in fnmatch.filter(f, "*.parquet"):
                    if filename != "latest.parquet":
                        bronze_files.append(os.path.join(r, filename))

    if not bronze_files:
        print("[Silver News] No Bronze Parquet files found. Exiting.")
        return

    print(f"[Silver News] Found {len(bronze_files)} Bronze Parquet files.")
    silver_path = os.path.join(ROOT, "data", "silver", "news_articles_enriched")
    tmp_json_dir = os.path.join(ROOT, "data", "silver", "_tmp_news_json")

    try:
        # 1. Watermark
        watermark = get_watermark(spark, silver_path)
        print(f"[Silver News] Watermark: {watermark}")

        # 2. Read incremental
        df = read_bronze_incremental(spark, bronze_files, watermark)
        if df.isEmpty():
            print("[Silver News] No new data. Skipping.")
            return

        # 3. Collect to Pandas (JVM collectToPython — stable, no casting)
        print("[Silver News] Collecting Bronze rows to driver...")
        pdf = df.toPandas()
        print(f"[Silver News] Collected {len(pdf)} rows.")

        # 4. Enrich on driver (pure Python — stable)
        pdf = enrich_on_driver(pdf)

        # 5. JSON BRIDGE: Save enriched data as JSON lines, read back with Spark
        print("[Silver News] Writing temp JSON (JSON Bridge)...")
        os.makedirs(tmp_json_dir, exist_ok=True)
        tmp_json_file = os.path.join(tmp_json_dir, "data.json")

        # Sanitize ALL NaN/NaT/None values to Python None before serialization
        # This prevents json.dumps(default=str) from writing "NaN" strings
        def sanitize_value(v):
            if v is None:
                return None
            if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                return None
            if isinstance(v, pd.Timestamp) and pd.isna(v):
                return None
            if isinstance(v, str) and v.strip().lower() in ('nan', 'nat', 'none', ''):
                return None
            return v

        records = pdf.to_dict(orient="records")
        with open(tmp_json_file, "w", encoding="utf-8") as f:
            for record in records:
                clean = {k: sanitize_value(v) for k, v in record.items()}
                f.write(json.dumps(clean, default=str) + "\n")

        print("[Silver News] Reading back via Spark (pure JVM)...")
        df_enriched = spark.read.json(tmp_json_file)

        # Safe timestamp casting — guard against any residual bad strings
        for ts_col in ["ingested_at", "published_at"]:
            if ts_col in df_enriched.columns:
                df_enriched = df_enriched.withColumn(
                    ts_col,
                    F.when(
                        F.col(ts_col).isNotNull() &
                        ~F.col(ts_col).isin("NaN", "None", "NaT", ""),
                        F.to_timestamp(F.col(ts_col))
                    ).otherwise(F.lit(None).cast(TimestampType()))
                )

        # 6. Write to Delta (pure JVM — stable)
        print("[Silver News] Writing to Silver Delta table...")
        write_history(spark, df_enriched, silver_path)
        print("[Silver News] Run Successful.")

    except Exception as e:
        print(f"[Silver News] FATAL: {str(e)}")
        raise e
    finally:
        # Clean up temp files
        import shutil
        if os.path.exists(tmp_json_dir):
            try:
                shutil.rmtree(tmp_json_dir)
            except Exception:
                pass
        spark.stop()


if __name__ == "__main__":
    main()