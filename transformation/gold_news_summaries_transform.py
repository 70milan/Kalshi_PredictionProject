import os
import sys

# PySpark Windows Fixes
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

_hadoop_home = os.environ.get("HADOOP_HOME", "")
if _hadoop_home:
    os.environ["PATH"] = os.path.join(_hadoop_home, "bin") + ";" + os.environ.get("PATH", "")

os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from delta import configure_spark_with_delta_pip

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def generate_news_summaries(spark, silver_path):
    if not os.path.exists(os.path.join(silver_path, "_delta_log")):
        print("[Gold News] Silver news table does not exist yet. Exiting.")
        return None

    df = spark.read.format("delta").load(silver_path)
    if "published_at" not in df.columns or "sentiment_score" not in df.columns:
        print("[Gold News] Missing required columns. Exiting.")
        return None

    # Filter out bad timestamps
    df = df.filter(F.col("published_at").isNotNull())
    df = df.withColumn("ts", F.col("published_at").cast("long"))

    # 1. Source Weighting (Prioritize full_text over title-only)
    if "sentiment_source" in df.columns:
        df = df.withColumn(
            "sentiment_weight",
            F.when(F.col("sentiment_source") == "full_text", F.lit(1.0))
             .when(F.col("sentiment_source") == "summary", F.lit(0.5))
             .when(F.col("sentiment_source") == "title", F.lit(0.2))
             .otherwise(F.lit(0.1))
        )
    else:
        df = df.withColumn("sentiment_weight", F.lit(1.0))

    df = df.withColumn("weighted_score", F.col("sentiment_score") * F.col("sentiment_weight"))

    # 2. Define Multi-Windows based on Unix Seconds
    win_15m = Window.partitionBy("source").orderBy("ts").rangeBetween(-900, 0)
    win_24h = Window.partitionBy("source").orderBy("ts").rangeBetween(-86400, 0)
    win_90d = Window.partitionBy("source").orderBy("ts").rangeBetween(-7776000, 0)

    # 3. Calculate Volume & Weighted Sentiment over Windows
    df_summary = df \
        .withColumn("vol_15m", F.count("ts").over(win_15m)) \
        .withColumn("vol_24h", F.count("ts").over(win_24h)) \
        .withColumn("vol_90d", F.count("ts").over(win_90d)) \
        .withColumn("sum_w_score_15m", F.sum("weighted_score").over(win_15m)) \
        .withColumn("sum_weight_15m", F.sum("sentiment_weight").over(win_15m)) \
        .withColumn("sum_w_score_24h", F.sum("weighted_score").over(win_24h)) \
        .withColumn("sum_weight_24h", F.sum("sentiment_weight").over(win_24h)) \
        .withColumn("sum_w_score_90d", F.sum("weighted_score").over(win_90d)) \
        .withColumn("sum_weight_90d", F.sum("sentiment_weight").over(win_90d))

    # Calculate final weighted average for each window
    df_summary = df_summary \
        .withColumn("sentiment_15m", F.when(F.col("sum_weight_15m") > 0, F.col("sum_w_score_15m") / F.col("sum_weight_15m")).otherwise(F.lit(0.0))) \
        .withColumn("sentiment_24h", F.when(F.col("sum_weight_24h") > 0, F.col("sum_w_score_24h") / F.col("sum_weight_24h")).otherwise(F.lit(0.0))) \
        .withColumn("sentiment_90d", F.when(F.col("sum_weight_90d") > 0, F.col("sum_w_score_90d") / F.col("sum_weight_90d")).otherwise(F.lit(0.0)))

    # 4. Aggregate to the Absolute Latest Snapshot per Source
    df_latest = df_summary.groupBy("source").agg(
        F.max("vol_15m").alias("vol_15m"),
        F.max("vol_24h").alias("vol_24h"),
        F.max("vol_90d").alias("vol_90d"),
        F.last("sentiment_15m").alias("sentiment_15m"),
        F.last("sentiment_24h").alias("sentiment_24h"),
        F.last("sentiment_90d").alias("sentiment_90d")
    )

    df_gold = df_latest.withColumn("ingested_at", F.current_timestamp())
    return df_gold

def main():
    print("[Gold News] Initializing PySpark Session for Narrative Volatility...")
    builder = SparkSession.builder \
        .appName("PredictIQ_Gold_News_Narrative") \
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
            return
            
        # We also create a history layer for the News Summaries to match GDELT and Kalshi
        history_path = gold_path + "_history"
        
        df_gold.write.format("delta").mode("append").option("mergeSchema", "true").save(history_path)
        df_gold.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(gold_path)
        
        row_count = df_gold.count()
        print(f"[Gold News] SUCCESS. Generated {row_count} Source Aggregates for Narrative Engine.")
        
    except Exception as e:
        print(f"[Gold News] FATAL ERROR: {str(e)}")
        raise e
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
