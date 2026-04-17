import os
import sys
from datetime import datetime

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

    # 1. Load Silver News
    df = spark.read.format("delta").load(silver_news_path)
    
    # Filter rows with missing published_at (now fixed in silver)
    df = df.filter(F.col("published_at").isNotNull())
    
    # Cast to timestamp then to long for windowing
    df = df.withColumn("ts", F.col("published_at").cast("timestamp").cast("long"))

    # 2. Define Windows
    win_15m = Window.partitionBy("source").orderBy("ts").rangeBetween(-900, 0)
    win_24h = Window.partitionBy("source").orderBy("ts").rangeBetween(-86400, 0)
    win_90d = Window.partitionBy("source").orderBy("ts").rangeBetween(-7776000, 0)

    # 3. Calculate Signals
    df_summary = df \
        .withColumn("vol_15m", F.count("link").over(win_15m)) \
        .withColumn("vol_24h", F.count("link").over(win_24h)) \
        .withColumn("vol_90d", F.count("link").over(win_90d)) \
        .withColumn("sent_15m", F.avg("sentiment_score").over(win_15m)) \
        .withColumn("sent_24h", F.avg("sentiment_score").over(win_24h))

    # 4. Final Aggregation per Source
    df_latest = df_summary.groupBy("source").agg(
        F.max("vol_15m").alias("vol_15m"),
        F.max("vol_24h").alias("vol_24h"),
        F.max("vol_90d").alias("vol_90d"),
        F.last("sent_15m").alias("sent_15m"),
        F.last("sent_24h").alias("sent_24h")
    )

    # 5. Spike Detection Signal (90 days baseline: 90*24*4 = 8640)
    df_gold = df_latest.withColumn(
        "vol_spike_multiplier", 
        F.col("vol_15m") / F.when(F.col("vol_90d") > 0, F.col("vol_90d") / 8640.0).otherwise(1.0)
    ).withColumn("ingested_at", F.current_timestamp())

    return df_gold

def main():
    print("[Gold News] Initializing PySpark Session...")
    builder = SparkSession.builder \
        .appName("PredictIQ_Gold_News_Summaries") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.parquet.enableVectorizedReader", "false")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    silver_path = os.path.join(ROOT, "data", "silver", "news_articles_enriched")
    gold_path = os.path.join(ROOT, "data", "gold", "news_summaries")

    try:
        df_gold = generate_news_summaries(spark, silver_path)
        if df_gold is None:
            print("[Gold News] No silver data found. Exiting.")
            return

        # Save History
        history_path = gold_path + "_history"
        df_gold.write.format("delta").mode("append").option("mergeSchema", "true").save(history_path)

        # Save Snapshot
        df_gold.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(gold_path)

        print(f"[Gold News] SUCCESS. Summarized {df_gold.count()} sources.")
        
    except Exception as e:
        print(f"[Gold News] FATAL ERROR: {str(e)}")
        raise e
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
