import os
import sys
from datetime import datetime, timezone, timedelta

# PySpark Windows Fixes
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from delta import configure_spark_with_delta_pip

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def generate_news_summaries(spark, silver_news_path):
    if not os.path.exists(os.path.join(silver_news_path, "_delta_log")):
        return None

    # Split computation to avoid running range windows over 90 days of articles:
    #   Pass A: vol_90d = GROUP BY count over 90 days (no range window, no sort)
    #   Pass B: vol_15m, vol_24h, sent windows = range windows on last 24h only
    # News has 6 sources so the broadcast join is trivial.

    cutoff_90d = (datetime.now(timezone.utc) - timedelta(days=90)).isoformat()
    cutoff_24h = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
    print(f"[Gold News] 90-day baseline cutoff: {cutoff_90d[:19]}")
    print(f"[Gold News] 24h window cutoff:      {cutoff_24h[:19]}")

    def load_news(cutoff):
        return spark.read.format("delta").load(silver_news_path) \
            .filter(F.col("ingested_at").cast("string") >= cutoff) \
            .filter(F.col("published_at").isNotNull()) \
            .withColumn("ts", F.col("published_at").cast("timestamp").cast("long"))

    # Pass A — 90-day baseline (simple aggregation, no window sort)
    df_baseline = load_news(cutoff_90d).groupBy("source").agg(
        F.count("link").alias("vol_90d")
    )
    df_baseline.cache()
    df_baseline.count()

    # Pass B — 24h range windows (small slice)
    df_24h = load_news(cutoff_24h)
    win_15m = Window.partitionBy("source").orderBy("ts").rangeBetween(-900, 0)
    win_24h = Window.partitionBy("source").orderBy("ts").rangeBetween(-86400, 0)

    df_summary = df_24h \
        .withColumn("vol_15m",  F.count("link").over(win_15m)) \
        .withColumn("vol_24h",  F.count("link").over(win_24h)) \
        .withColumn("sent_15m", F.avg("sentiment_score").over(win_15m)) \
        .withColumn("sent_24h", F.avg("sentiment_score").over(win_24h))

    df_latest = df_summary.groupBy("source").agg(
        F.max("vol_15m").alias("vol_15m"),
        F.max("vol_24h").alias("vol_24h"),
        F.last("sent_15m").alias("sent_15m"),
        F.last("sent_24h").alias("sent_24h"),
    ).join(F.broadcast(df_baseline), on="source", how="left").fillna({"vol_90d": 0})

    # 5. Spike Detection Signal (90 days baseline: 90*24*4 = 8640)
    # Require at least 100 baseline articles before trusting the spike signal (news sources
    # publish many articles; fewer than 100 over 90 days means the source is too sparse).
    # Cap at 10x to prevent cold-start inflation.
    MIN_BASELINE = 100
    df_gold = df_latest.withColumn(
        "vol_spike_multiplier",
        F.when(
            F.col("vol_90d") >= MIN_BASELINE,
            F.least(F.lit(10.0), F.col("vol_15m") / (F.col("vol_90d") / 8640.0))
        ).otherwise(F.lit(0.0))
    ).withColumn("ingested_at", F.current_timestamp())

    return df_gold

def run(spark):
    """Runs the transform using a caller-managed SparkSession (no spark.stop)."""
    silver_path = os.path.join(ROOT, "data", "silver", "news_articles_enriched")
    gold_path = os.path.join(ROOT, "data", "gold", "news_summaries")

    df_gold = generate_news_summaries(spark, silver_path)
    if df_gold is None:
        print("[Gold News] No silver data found. Exiting.")
        return

    # Cache before 3 uses (history append, current overwrite, count) — avoids recompute
    df_gold.cache()
    row_count = df_gold.count()

    # Save History
    history_path = gold_path + "_history"
    df_gold.write.format("delta").mode("append").option("mergeSchema", "true").save(history_path)

    # Save Snapshot
    df_gold.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(gold_path)

    df_gold.unpersist()
    print(f"[Gold News] SUCCESS. Summarized {row_count} sources.")


def main():
    print("[Gold News] Initializing PySpark Session...")
    builder = SparkSession.builder \
        .appName("PredictIQ_Gold_News_Summaries") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "16") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.minPartitionSize", "32mb") \
        .config("spark.sql.files.maxPartitionBytes", "128mb") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.parquet.enableVectorizedReader", "false")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    try:
        run(spark)
    except Exception as e:
        print(f"[Gold News] FATAL ERROR: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
