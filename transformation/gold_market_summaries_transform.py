import os
import sys

# PySpark Windows Fixes
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

_hadoop_home = os.environ.get("HADOOP_HOME", "")
if _hadoop_home:
    os.environ["PATH"] = os.path.join(_hadoop_home, "bin") + ";" + os.environ.get("PATH", "")

os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def calculate_odds_deltas(df):
    """
    Calculates velocity metrics over 15-minute, 1-hour, and 24-hour windows.
    Assumes data is ingested every 15 minutes.
    """
    # Ensure yes_bid and volume are double types for clean math
    cols = df.columns
    if "yes_bid" in cols:
        df = df.withColumn("yes_bid", F.col("yes_bid").cast("double"))
    else:
        df = df.withColumn("yes_bid", F.lit(0.0).cast("double"))
        
    if "volume" in cols:
        df = df.withColumn("volume", F.col("volume").cast("double"))
    else:
        df = df.withColumn("volume", F.lit(0.0).cast("double"))

    window_spec = Window.partitionBy("ticker").orderBy("ingested_at")
    
    # Use lag exactly by 1 (15m), 4 (1h), and 96 (24h)
    df = df.withColumn("prev_yes_bid_15m", F.lag("yes_bid", 1).over(window_spec))
    df = df.withColumn("prev_yes_bid_1h", F.lag("yes_bid", 4).over(window_spec))
    df = df.withColumn("prev_yes_bid_24h", F.lag("yes_bid", 96).over(window_spec))
    
    # Calculate percentage deltas (safeguard division by zero)
    df = df.withColumn(
        "delta_15m", 
        F.when(F.col("prev_yes_bid_15m") > 0, (F.col("yes_bid") - F.col("prev_yes_bid_15m")) / F.col("prev_yes_bid_15m")).otherwise(F.lit(0.0))
    )
    df = df.withColumn(
        "delta_1h", 
        F.when(F.col("prev_yes_bid_1h") > 0, (F.col("yes_bid") - F.col("prev_yes_bid_1h")) / F.col("prev_yes_bid_1h")).otherwise(F.lit(0.0))
    )
    df = df.withColumn(
        "delta_24h", 
        F.when(F.col("prev_yes_bid_24h") > 0, (F.col("yes_bid") - F.col("prev_yes_bid_24h")) / F.col("prev_yes_bid_24h")).otherwise(F.lit(0.0))
    )
    
    return df

def generate_market_summaries(spark, history_path):
    if not os.path.exists(os.path.join(history_path, "_delta_log")):
        print("[Gold Kalshi] Silver history table does not exist yet. Exiting.")
        return None
        
    df = spark.read.format("delta").load(history_path)
    
    # Drop rows without ticker or ingested_at
    if "ticker" not in df.columns or "ingested_at" not in df.columns:
        print("[Gold Kalshi] Missing critical columns in silver table. Exiting.")
        return None
        
    df = df.filter(F.col("ticker").isNotNull() & F.col("ingested_at").isNotNull())
    
    # Calculate deltas over the history
    df_with_deltas = calculate_odds_deltas(df)
    
    # Filter down to just the Absolute Latest snapshot per ticker
    window_latest = Window.partitionBy("ticker").orderBy(F.col("ingested_at").desc())
    df_latest = df_with_deltas.withColumn("rn", F.row_number().over(window_latest)).filter(F.col("rn") == 1).drop("rn")
    
    # Implement V4 Staleness Guard: > 20 mins means stale
    df_final = df_latest.withColumn(
        "data_age_mins",
        (F.unix_timestamp(F.current_timestamp()) - F.unix_timestamp(F.col("ingested_at"))) / 60.0
    ).withColumn(
        "is_stale",
        F.col("data_age_mins") > 20.0
    )
    
    # Select clean gold schema
    all_cols = df_final.columns
    df_gold = df_final.select(
        "ticker",
        F.col("title") if "title" in all_cols else F.lit(None).cast("string").alias("title"),
        F.col("status") if "status" in all_cols else F.lit(None).cast("string").alias("status"),
        F.col("close_date") if "close_date" in all_cols else F.lit(None).cast("string").alias("close_date"),
        F.col("volume").alias("current_vol"),
        F.col("yes_bid").alias("current_yes_bid"),
        F.col("yes_ask") if "yes_ask" in all_cols else F.lit(None).cast("double").alias("current_yes_ask"),
        "delta_15m",
        "delta_1h",
        "delta_24h",
        "ingested_at",
        "is_stale"
    )
    return df_gold

def main():
    print("[Gold Kalshi] Initializing PySpark Session for Market Summaries...")
    ivy_dir = os.environ.get("IVY_PACKAGE_DIR", "")
    builder = SparkSession.builder \
        .appName("PredictIQ_Gold_Market_Summaries") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.parquet.enableVectorizedReader", "false")
    if ivy_dir:
        builder = builder.config("spark.jars.ivy", ivy_dir)

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    silver_history = os.path.join(ROOT, "data", "silver", "kalshi_markets_history")
    gold_path = os.path.join(ROOT, "data", "gold", "market_summaries")

    try:
        df_gold = generate_market_summaries(spark, silver_history)
        if df_gold is None:
            return
            
        row_count = df_gold.count()
        print(f"[Gold Kalshi] Generated {row_count} Market Summaries. Writing to Delta...")
        
        # Overwrite mode because gold serves the absolute latest snapshot state to the presentation layer
        df_gold.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(gold_path)
        print("[Gold Kalshi] SUCCESS. Market Summaries table completely updated.")
        
    except Exception as e:
        print(f"[Gold Kalshi] FATAL ERROR: {str(e)}")
        raise e
    finally:
        spark.stop()

if __name__ == "__main__":
    import time
    import traceback

    print("[Gold Kalshi] Docker Polling Service Initialized (5-min intervals).")
    while True:
        try:
            main()
            print("[Gold Kalshi] Run complete. Sleeping for 300 seconds...")
        except Exception:
            print("[Gold Kalshi] LOOP ERROR detected:")
            traceback.print_exc()
            print("[Gold Kalshi] Sleeping for 300 seconds before retry...")
        
        time.sleep(300)
