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

_STOPWORDS = {
    "will", "the", "this", "that", "with", "from", "have", "been",
    "than", "more", "about", "what", "when", "where", "which", "their",
    "they", "would", "could", "should", "after", "before", "into", "over",
    "make", "some", "just", "like", "then", "also", "only", "says", "said",
    "week", "next", "last", "year", "amid", "ahead",
    # news format words — titles, not topics
    "watch", "live", "exclusive", "report", "breaking", "video",
    "podcast", "listen", "photos", "opinion", "analysis", "explainer",
    "here", "your", "there", "these", "those", "have", "been",
    "first", "know", "says", "sign", "open",
}

def generate_news_summaries(spark, silver_news_path):
    if not os.path.exists(os.path.join(silver_news_path, "_delta_log")):
        return None

    cutoff_90d = (datetime.now(timezone.utc) - timedelta(days=90)).isoformat()
    cutoff_24h = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
    print(f"[Gold News] 90-day baseline cutoff: {cutoff_90d[:19]}")
    print(f"[Gold News] 24h window cutoff:      {cutoff_24h[:19]}")

    def load_news_keywords(cutoff):
        return spark.read.format("delta").load(silver_news_path) \
            .filter(F.col("ingested_at").cast("string") >= cutoff) \
            .filter(F.col("published_at").isNotNull()) \
            .filter(F.col("title").isNotNull()) \
            .withColumn("ts", F.col("published_at").cast("timestamp").cast("long")) \
            .withColumn("clean_title", F.lower(F.regexp_replace(F.col("title"), "[^a-zA-Z0-9 ]", ""))) \
            .withColumn("keyword", F.explode(F.split(F.col("clean_title"), " "))) \
            .filter(F.length(F.col("keyword")) > 3) \
            .filter(~F.col("keyword").isin(list(_STOPWORDS))) \
            .filter(~F.col("keyword").rlike("^[0-9]"))

    # Pass A — 90-day baseline per (feed_key, keyword) for spike normalisation
    df_baseline = load_news_keywords(cutoff_90d).groupBy("feed_key", "keyword").agg(
        F.count("link").alias("vol_90d")
    )
    df_baseline.cache()
    df_baseline.count()

    # Pass B — 24h range windows partitioned by (feed_key, keyword)
    df_24h = load_news_keywords(cutoff_24h)

    win_15m = Window.partitionBy("feed_key", "keyword").orderBy("ts").rangeBetween(-900, 0)
    win_24h = Window.partitionBy("feed_key", "keyword").orderBy("ts").rangeBetween(-86400, 0)

    df_summary = df_24h \
        .withColumn("vol_15m",  F.count("link").over(win_15m)) \
        .withColumn("vol_24h",  F.count("link").over(win_24h)) \
        .withColumn("tone_15m", F.avg("sentiment_score").over(win_15m)) \
        .withColumn("tone_24h", F.avg("sentiment_score").over(win_24h))

    # One row per (feed_key, keyword) — latest snapshot of each window
    df_latest = df_summary.groupBy("feed_key", "keyword").agg(
        F.max("vol_15m").alias("vol_15m"),
        F.max("vol_24h").alias("vol_24h"),
        F.last("tone_15m").alias("tone_15m"),
        F.last("tone_24h").alias("tone_24h"),
    ).join(F.broadcast(df_baseline), on=["feed_key", "keyword"], how="left").fillna({"vol_90d": 0})

    # Cross-source divergence + consensus per keyword.
    # tone_divergence: spread between most positive and most negative source on this keyword.
    # tone_consensus:  fraction of sources agreeing on direction (1.0 = unanimous, 0.5 = split).
    df_cross = df_latest.groupBy("keyword").agg(
        F.max("tone_15m").alias("_max_tone"),
        F.min("tone_15m").alias("_min_tone"),
        F.count("feed_key").alias("_src_count"),
        F.sum(F.when(F.col("tone_15m") < 0, 1).otherwise(0)).alias("_neg_src"),
        F.sum(F.when(F.col("tone_15m") > 0, 1).otherwise(0)).alias("_pos_src"),
    )
    df_latest = df_latest.join(df_cross, on="keyword", how="left") \
        .withColumn("tone_divergence", F.col("_max_tone") - F.col("_min_tone")) \
        .withColumn(
            "tone_consensus",
            F.greatest(F.col("_neg_src"), F.col("_pos_src")) / F.col("_src_count")
        ) \
        .drop("_max_tone", "_min_tone", "_src_count", "_neg_src", "_pos_src")

    # Spike detection — keyword-specific volume now vs 90d baseline rate
    MIN_BASELINE = 3
    df_gold = df_latest.withColumn(
        "vol_spike_multiplier",
        F.when(
            F.col("vol_90d") >= MIN_BASELINE,
            F.least(F.lit(10.0), F.col("vol_15m") / (F.col("vol_90d") / 8640.0))
        ).otherwise(F.lit(0.0))
    ).withColumn("ingested_at", F.current_timestamp())

    return df_gold

def run(spark):
    """Runs the transform using a caller-managed SparkSession."""
    silver_path = os.path.join(ROOT, "data", "silver", "news_articles_enriched")
    gold_path = os.path.join(ROOT, "data", "gold", "news_summaries")

    df_gold = generate_news_summaries(spark, silver_path)
    if df_gold is None:
        print("[Gold News] No silver data found. Exiting.")
        return

    df_gold.cache()
    row_count = df_gold.count()

    # Save History
    history_path = gold_path + "_history"
    df_gold.write.format("delta").mode("append").option("mergeSchema", "true").save(history_path)

    # Save Snapshot
    df_gold.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(gold_path)

    df_gold.unpersist()
    print(f"[Gold News] SUCCESS. Computed keyword-level signals for {row_count} (feed, keyword) pairs.")


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
