import os
import sys
import fnmatch
import json
import math
import pandas as pd
import numpy as np
from datetime import datetime, timezone
from dateutil import parser

# Force PySpark workers to use the exact virtual environment Python executable
# Using legacy 8.3 Short Path to avoid spaces that break Spark .cmd files on Windows
SHORT_PYTHON = "C:/DATAEN~1/codeprep/PREDEC~1/.venv/Scripts/python.exe"
os.environ["PYSPARK_PYTHON"] = SHORT_PYTHON
os.environ["PYSPARK_DRIVER_PYTHON"] = SHORT_PYTHON

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

def get_source_watermarks(spark, silver_path):
    """Returns a dictionary of {source: max_ingested_at} from the Silver table."""
    watermarks = {}
    delta_log = os.path.join(silver_path, "_delta_log")
    if os.path.exists(delta_log):
        try:
            # We use distinct sources to get the watermark for each one
            df = spark.read.format("delta").load(silver_path)
            res = df.groupBy("source").agg(F.max("ingested_at")).collect()
            for row in res:
                if row[0]: # Handle potential null source
                    watermarks[row[0].strip().lower()] = row[1]
        except Exception:
            pass
    return watermarks


def read_bronze_incremental(spark, news_sources, ROOT, watermarks):
    """Read Bronze Parquet files source-by-source, applying source-specific watermarks."""
    dfs = []
    
    for source in news_sources:
        source_dir = os.path.join(ROOT, "data", "bronze", source)
        if not os.path.isdir(source_dir):
            continue
            
        # Map folder names to database source names to fix watermark lookup
        SOURCE_MAP = {
            "foxnews": "fox news",
            "nypost": "ny post",
            "hindu": "the hindu"
        }
        
        # Apply source-specific watermark
        base_source = source.strip().lower()
        source_key = SOURCE_MAP.get(base_source, base_source)
        
        watermark_ts = 0
        if source_key in watermarks:
            try:
                wm_dt = parser.parse(str(watermarks[source_key]))
                if wm_dt.tzinfo is None:
                    wm_dt = wm_dt.replace(tzinfo=timezone.utc)
                # 1h safety buffer: never skip a file due to mtime/ingested_at
                # clock slop. The row-level filter dedups the overlap exactly.
                watermark_ts = wm_dt.timestamp() - 3600
            except Exception:
                pass
                
        # Discover files for this specific source
        source_files = []
        for r, d, f in os.walk(source_dir):
            for filename in fnmatch.filter(f, "*.parquet"):
                if filename == "latest.parquet":
                    continue
                filepath = os.path.join(r, filename)
                try:
                    if os.path.getmtime(filepath) > watermark_ts:
                        source_files.append(filepath)
                except OSError:
                    continue
        
        if not source_files:
            continue
            
        # Read files for this source
        df_source = spark.read.option("mergeSchema", "true").parquet(*source_files)
        
        if source_key in watermarks:
            wm = str(watermarks[source_key])
            # Compare as real timestamps, not strings. Bronze ingested_at uses an
            # ISO 'T' separator; the Silver-derived watermark uses a space. Raw
            # string comparison sorted 'T' (84) above ' ' (32), so every same-day
            # article re-passed every cycle — re-appending ~7800 dupes per run.
            df_source = df_source.filter(
                F.to_timestamp(F.col("ingested_at")) > F.to_timestamp(F.lit(wm))
            )
            
        dfs.append(df_source)
        
    if not dfs:
        return None
        
    # Union all sources into one master DataFrame
    master_df = dfs[0]
    for next_df in dfs[1:]:
        master_df = master_df.unionByName(next_df, allowMissingColumns=True)
        
    return master_df


def write_history(spark, df, silver_path):
    """Append enriched rows to Silver Delta table. Idempotency is handled by the Source-Level Watermarks."""
    print(f"[Silver News] Appending {df.count()} rows to Delta at {silver_path}...")
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

    # 2. Robust date parsing
    # RSS and News APIs use a variety of columns for dates. Normalize them all.
    date_cols = ["published_at", "pubDate", "pubdate", "published", "date", "created_at"]
    
    def parse_dt(row):
        # Try every possible column
        for c in date_cols:
            val = row.get(c)
            if val and str(val).strip():
                try:
                    # dateutil parses: ISO, RSS (RFC822), Month Day Year, etc.
                    dt = parser.parse(str(val))
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    return dt.strftime("%Y-%m-%d %H:%M:%S")
                except:
                    continue
        return None

    print("[Silver News] Parsing dates with dateutil...")
    pdf["published_at"] = pdf.apply(parse_dt, axis=1)
    
    # Ingested_at is usually ISO or a standard string, but let's be safe
    pdf["ingested_at"] = pdf["ingested_at"].apply(
        lambda x: parser.parse(str(x)).strftime("%Y-%m-%d %H:%M:%S") if x else None
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

def run(spark):
    """Runs the transform using a caller-managed SparkSession (no spark.stop)."""
    news_sources = ["bbc", "cnn", "foxnews", "nypost", "nyt", "thehindu"]
    silver_path = os.path.join(ROOT, "data", "silver", "news_articles_enriched")
    tmp_json_dir = os.path.join(ROOT, "data", "silver", "_tmp_news_json")

    try:
        # 1. Source-Level Watermarks
        watermarks = get_source_watermarks(spark, silver_path)
        print(f"[Silver News] Current source watermarks: {list(watermarks.keys())}")

        # 2. Read incremental (Source-by-Source)
        df = read_bronze_incremental(spark, news_sources, ROOT, watermarks)
        if df is None or df.isEmpty():
            print("[Silver News] No new data found across all sources. Skipping.")
            return

        # 3. Collect to Pandas (stable JVM bridge)
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
                        F.col(ts_col).cast(TimestampType())
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
    try:
        run(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()