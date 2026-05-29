import os
import sys
import fnmatch
import json
import math
import shutil
import pandas as pd
import numpy as np
from datetime import datetime, timezone
from dateutil import parser

SHORT_PYTHON = "C:/DATAEN~1/codeprep/PREDEC~1/.venv/Scripts/python.exe"
os.environ["PYSPARK_PYTHON"] = SHORT_PYTHON
os.environ["PYSPARK_DRIVER_PYTHON"] = SHORT_PYTHON

if os.environ.get("HADOOP_HOME", ""):
os.environ["PATH"] = os.path.join(os.environ["HADOOP_HOME"], "bin") + ";" + os.environ.get("PATH", "")

os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, TimestampType
from delta import configure_spark_with_delta_pip
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

print("starting")
ivy_dir = os.environ.get("IVY_PACKAGE_DIR", "")
builder = (
SparkSession.builder
.appName("PredictIQ_Silver_News")
.config("spark.driver.memory", "4g")
.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
.config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
.config("spark.driver.memory", "6g")  # bump to 6-8g if you have headroom
.config("spark.sql.shuffle.partitions", "4")  # notebook on single machine = low parallelism
.config("spark.default.parallelism", "4")
)
if ivy_dir:
    builder = builder.config("spark.jars.ivy", ivy_dir)
print("started")

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

silver_path = os.path.join(ROOT, "data", "silver", "news_articles_enriched")
tmp_json_dir = os.path.join(ROOT, "data", "silver", "_tmp_news_json")

watermarks = {}
delta_log = os.path.join(silver_path, "_delta_log")
if os.path.exists(delta_log):
    df = spark.read.format("delta").load(silver_path)
    res = df.groupBy("feed_key").agg(F.max("ingested_at")).collect()
    for row in res:
        if row[0]:
            watermarks[row[0].strip().lower()] = row[1]

print(f"[Silver News] Current feed key watermarks: {list(watermarks.keys())}")

dfs = []
bronze_news_base = os.path.join(ROOT, "bronze", "news")
if os.path.isdir(bronze_news_base):
    news_feeds = [d for d in os.listdir(bronze_news_base) if os.path.isdir(os.path.join(bronze_news_base, d)) and d != "congress_bills"]
    print(f"[Silver News] Discovered {len(news_feeds)} active media feed keys.")

    for feed_key in news_feeds:
        source_dir = os.path.join(bronze_news_base, feed_key)
        source_key = feed_key.strip().lower()
        watermark_ts = 0
        if source_key in watermarks:
            wm_dt = parser.parse(str(watermarks[source_key]))
            if wm_dt.tzinfo is None:
                wm_dt = wm_dt.replace(tzinfo=timezone.utc)
            watermark_ts = wm_dt.timestamp() - 3600

        source_files = []
        for r, d, f in os.walk(source_dir):
            for filename in fnmatch.filter(f, "*.parquet"):
                if filename == "latest.parquet":
                    continue
                filepath = os.path.join(r, filename)
                if os.path.exists(filepath) and os.path.getmtime(filepath) > watermark_ts:
                    source_files.append(filepath)

        if source_files:
            df_source = spark.read.option("mergeSchema", "true").parquet(*source_files)
            if source_key in watermarks:
                df_source = df_source.filter(F.to_timestamp(F.col("ingested_at")) > F.to_timestamp(F.lit(str(watermarks[source_key]))))
            dfs.append(df_source)

    if not dfs:
        print("[Silver News] No new data found. Skipping.")
    else:
        master_df = dfs[0]
        for next_df in dfs[1:]:
            master_df = master_df.unionByName(next_df, allowMissingColumns=True)

        print("[Silver News] Collecting Bronze rows to driver...")
        pdf = master_df.toPandas()

        if len(pdf) > 0:
            for col in ["pubDate", "published"]:
                if col in pdf.columns:
                    pdf["published_at"] = pdf.get("published_at", pd.Series(dtype=object)).fillna(pdf[col])
                    pdf.drop(columns=[col], inplace=True)

            date_cols = ["published_at", "pubdate", "date", "created_at"]
            for idx, row in pdf.iterrows():
                for c in date_cols:
                    val = row.get(c) if c in row.index else None
                    if val and str(val).strip():
                        dt = parser.parse(str(val))
                        pdf.at[idx, "published_at"] = (dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt).strftime("%Y-%m-%d %H:%M:%S")
                        break
                if pd.isna(pdf.at[idx, "published_at"]):
                    pdf.at[idx, "published_at"] = None

            pdf["ingested_at"] = pdf["ingested_at"].apply(lambda x: parser.parse(str(x)).strftime("%Y-%m-%d %H:%M:%S") if x else None)

            for col in ["summary", "full_text", "title"]:
                pdf[col] = pdf.get(col, pd.Series(dtype=str)).fillna("").astype(str)

            analyzer = SentimentIntensityAnalyzer()
            sentiment_scores = []
            sentiment_labels = []
            for idx, row in pdf.iterrows():
                text = row["full_text"] if len(row["full_text"].strip()) > 0 else (row["summary"] if len(row["summary"].strip()) > 0 else row["title"])
                score = float(analyzer.polarity_scores(text)["compound"])
                label = "Positive" if score >= 0.05 else ("Negative" if score <= -0.05 else "Neutral")
                sentiment_scores.append(score)
                sentiment_labels.append(label)

            pdf["sentiment_score"] = sentiment_scores
            pdf["sentiment_label"] = sentiment_labels

            os.makedirs(tmp_json_dir, exist_ok=True)
            tmp_json_file = os.path.join(tmp_json_dir, "data.json")

            with open(tmp_json_file, "w", encoding="utf-8") as f:
                for record in pdf.to_dict(orient="records"):
                    clean = {}
                    for k, v in record.items():
                        if v is None or (isinstance(v, float) and (math.isnan(v) or math.isinf(v))) or (isinstance(v, str) and v.lower() in ('nan', 'nat', 'none', '')):
                            clean[k] = None
                        else:
                            clean[k] = v
                    f.write(json.dumps(clean, default=str) + "\n")

            df_enriched = spark.read.json(tmp_json_file)
            for ts_col in ["ingested_at", "published_at"]:
                if ts_col in df_enriched.columns:
                    df_enriched = df_enriched.withColumn(ts_col, F.when(F.col(ts_col).isNotNull() & ~F.col(ts_col).isin("NaN", "None", "NaT", ""), F.col(ts_col).cast(TimestampType())).otherwise(F.lit(None).cast(TimestampType())))

            print(f"[Silver News] Appending {df_enriched.count()} rows to Delta...")
            df_enriched.write.format("delta").mode("append").option("mergeSchema", "true").save(silver_path)
            print("[Silver News] SUCCESS.")