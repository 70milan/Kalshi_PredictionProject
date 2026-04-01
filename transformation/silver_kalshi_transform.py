from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import os

# Milan's Machine PySpark Config
spark = SparkSession.builder \
    .appName("PredictIQ_Silver_Kalshi") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Pathing
PROJECT_ROOT = "."
BRONZE_PATH  = os.path.join(PROJECT_ROOT, "data/bronze/kalshi_markets/open/*.parquet")
REFERENCE_PATH = os.path.join(PROJECT_ROOT, "reference/kalshi_series_map.csv")
SILVER_CURRENT_PATH = os.path.join(PROJECT_ROOT, "data/silver/kalshi_markets_current")
SILVER_HISTORY_PATH = os.path.join(PROJECT_ROOT, "data/silver/kalshi_markets_history")

print(f"📡 Reading Kalshi Bronze from: {BRONZE_PATH}")
df = spark.read.parquet(BRONZE_PATH)

# Enrichment with Series Map if exists
if os.path.exists(REFERENCE_PATH):
    print("📋 Joining with kalshi_series_map.csv...")
    ref_df = spark.read.csv(REFERENCE_PATH, header=True, inferSchema=True)
    df = df.join(ref_df, on="ticker", how="left")
else:
    print("⚠️ No kalshi_series_map.csv found. Skipping join.")

# 1. Current State Table (Latest odds per ticker)
# Get the most recent snapshot per ticker based on ingested_at
df_current = df.withColumn("rank", F.row_number().over(
    F.Window.partitionBy("ticker").orderBy(F.desc("ingested_at"))
)).filter(F.col("rank") == 1).drop("rank")

print(f"💾 Writing Current State to: {SILVER_CURRENT_PATH} (Overwrite)")
df_current.write.format("delta").mode("overwrite").save(SILVER_CURRENT_PATH)

# 2. History Table (Append mode)
print(f"💾 Appending to History at: {SILVER_HISTORY_PATH} (Append)")
df.write.format("delta").mode("append").save(SILVER_HISTORY_PATH)

print("✅ Kalshi Silver transformation complete.")
spark.stop()
