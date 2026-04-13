import os
import sys
from datetime import datetime

# PySpark Windows Fixes
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

_hadoop_home = os.environ.get("HADOOP_HOME", "")
if _hadoop_home:
    os.environ["PATH"] = os.path.join(_hadoop_home, "bin") + ";" + os.environ.get("PATH", "")

os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta import configure_spark_with_delta_pip

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def generate_news_summaries(spark, silver_path):
    if not os.path.exists(os.path.join(silver_path, "_delta_log")):
        print("[Gold News Summaries] Silver news table does not exist yet. Exiting.")
        return None

    # Calculate 7-day cutoff
    cutoff = F.current_timestamp() - F.expr("INTERVAL 7 DAYS")

    df = spark.read.format("delta").load(silver_path)
    
    # Filter for recent data
    df_7d = df.filter(
        (F.col("published_at").isNotNull()) & 
        (F.to_timestamp(F.col("published_at")) > cutoff)
    )

    if df_7d.isEmpty():
        print("[Gold News Summaries] No recent news data found.")
        return None

    # Aggregate by Source
    df_gold = df_7d.groupBy("source").agg(
        F.count("title").alias("rolling_7d_volume"),
        F.avg("sentiment_score").alias("rolling_7d_sentiment"),
        F.stddev("sentiment_score").alias("sentiment_instability")
    )

    # Add metadata
    df_gold = df_gold.withColumn("ingested_at", F.current_timestamp())

    return df_gold

def main():
    print("[Gold News] Initializing PySpark Session for Media Bias Summaries...")
    ivy_dir = os.environ.get("IVY_PACKAGE_DIR", "")
    builder = SparkSession.builder \
        .appName("PredictIQ_Gold_News_Summaries") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.parquet.enableVectorizedReader", "false")
    if ivy_dir:
        builder = builder.config("spark.jars.ivy", ivy_dir)

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    silver_path = os.path.join(ROOT, "data", "silver", "news_articles_enriched")
    gold_path = os.path.join(ROOT, "data", "gold", "news_summaries")

    try:
        df_gold = generate_news_summaries(spark, silver_path)
        if df_gold is None:
            return
            
        row_count = df_gold.count()
        print(f"[Gold News] Generated {row_count} Source Aggregates. Writing to Delta...")
        
        df_gold.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(gold_path)
        print("[Gold News] SUCCESS. Media Bias Summaries table completely updated.")
        
    except Exception as e:
        print(f"[Gold News] FATAL ERROR: {str(e)}")
        raise e
    finally:
        spark.stop()

if __name__ == "__main__":
    main()

