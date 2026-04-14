import os
import sys
from datetime import datetime, timezone

# PySpark Windows Fixes
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from delta import configure_spark_with_delta_pip

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def generate_gdelt_summaries(spark, gkg_path, events_path):
    # Check if silver tables exist
    log_gkg = os.path.exists(os.path.join(gkg_path, "_delta_log"))
    log_events = os.path.exists(os.path.join(events_path, "_delta_log"))
    
    if not (log_gkg or log_events):
        print("[Gold GDELT] No silver history found. Exiting.")
        return None

    # 1. Load and Standardize GKG Entities
    # We extract the 'Fingerprint' (Person, Theme, Org, Location)
    df_gkg = spark.read.format("delta").load(gkg_path) \
        .select("ingested_at", "raw_tone_stats", "persons_array", "themes_array", "orgs_array", "locs_array")
    
    # Extract tone (1st value in raw_tone_stats)
    df_gkg = df_gkg.withColumn("tone", F.split(F.col("raw_tone_stats"), ",").getItem(0).cast("double"))
    df_gkg = df_gkg.withColumn("ts", F.col("ingested_at").cast("long"))

    # Create entity-grain rows
    gkg_entities = []
    
    # Persons
    gkg_entities.append(df_gkg.select("ts", "tone", F.explode("persons_array").alias("entity_name"), F.lit("person").alias("entity_type")))
    # Themes
    gkg_entities.append(df_gkg.select("ts", "tone", F.explode("themes_array").alias("entity_name"), F.lit("theme").alias("entity_type")))
    # Organizations
    gkg_entities.append(df_gkg.select("ts", "tone", F.explode("orgs_array").alias("entity_name"), F.lit("organization").alias("entity_type")))
    # Locations
    gkg_entities.append(df_gkg.select("ts", "tone", F.explode("locs_array").alias("entity_name"), F.lit("location").alias("entity_type")))

    # Union all GKG entities
    df_all = gkg_entities[0]
    for df in gkg_entities[1:]:
        df_all = df_all.unionAll(df)

    # Clean up names (alpha-numeric + spaces)
    df_all = df_all.filter(F.col("entity_name").rlike("^[A-Za-z0-9 _]+$"))

    # 2. Add Multi-Window Velocity Signals
    # Define Windows using unix seconds
    win_15m = Window.partitionBy("entity_type", "entity_name").orderBy("ts").rangeBetween(-900, 0)
    win_24h = Window.partitionBy("entity_type", "entity_name").orderBy("ts").rangeBetween(-86400, 0)
    win_90d = Window.partitionBy("entity_type", "entity_name").orderBy("ts").rangeBetween(-7776000, 0)

    # Calculate Volume Velocity (Count of mentions in windows)
    df_summary = df_all \
        .withColumn("vol_15m", F.count("ts").over(win_15m)) \
        .withColumn("vol_24h", F.count("ts").over(win_24h)) \
        .withColumn("vol_90d", F.count("ts").over(win_90d)) \
        .withColumn("tone_15m", F.avg("tone").over(win_15m)) \
        .withColumn("tone_24h", F.avg("tone").over(win_24h))

    # 3. Aggregate to latest state per entity
    # We only care about the most recent results for the 'current' snapshot
    df_latest = df_summary.groupBy("entity_type", "entity_name").agg(
        F.max("vol_15m").alias("vol_15m"),
        F.max("vol_24h").alias("vol_24h"),
        F.max("vol_90d").alias("vol_90d"),
        F.last("tone_15m").alias("tone_15m"),
        F.last("tone_24h").alias("tone_24h")
    )

    # 4. Spike Detection Signal
    # Velocity = Current / Baseline (avoid div by zero)
    df_latest = df_latest.withColumn(
        "vol_spike_multiplier", 
        F.col("vol_15m") / F.when(F.col("vol_90d") > 0, F.col("vol_90d") / (90 * 24 * 4)).otherwise(1.0)
    )

    df_gold = df_latest.withColumn("ingested_at", F.current_timestamp())
    return df_gold

def main():
    print("[Gold GDELT] Initializing PySpark Session for Velocity Summaries...")
    builder = SparkSession.builder \
        .appName("PredictIQ_Gold_GDELT_Velocity") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.parquet.enableVectorizedReader", "false")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    gkg_path = os.path.join(ROOT, "data", "silver", "gdelt_gkg_history")
    events_path = os.path.join(ROOT, "data", "silver", "gdelt_events_history")
    gold_path = os.path.join(ROOT, "data", "gold", "gdelt_summaries")

    try:
        df_gold = generate_gdelt_summaries(spark, gkg_path, events_path)
        if df_gold is None: return

        # Save History (Append)
        history_path = gold_path + "_history"
        df_gold.write.format("delta").mode("append").option("mergeSchema", "true").save(history_path)

        # Save Current (Overwrite)
        df_gold.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(gold_path)
        
        print(f"[Gold GDELT] SUCCESS. Velocity signals generated for {df_gold.count()} entities.")
        
    except Exception as e:
        print(f"[Gold GDELT] FATAL ERROR: {str(e)}")
        raise e
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
