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

def generate_gdelt_summaries(spark, gkg_path, events_path):
    # Check if silver tables exist
    log_gkg = os.path.exists(os.path.join(gkg_path, "_delta_log"))
    log_events = os.path.exists(os.path.join(events_path, "_delta_log"))
    
    if not (log_gkg or log_events):
        print("[Gold GDELT] No silver history found. Exiting.")
        return None

    # Split computation into two passes to avoid running range windows over 90 days
    # of data (which produces 300K+ partitioned rows in the window sort):
    #   Pass A: vol_90d = simple GROUP BY count over 90 days (no range window, no sort)
    #   Pass B: vol_15m, vol_24h, tone windows = range windows on last 24h only (tiny slice)
    # Join A + B at entity level. Same result, ~10x less data in the expensive window pass.

    cutoff_90d = (datetime.now(timezone.utc) - timedelta(days=90)).isoformat()
    cutoff_24h = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
    print(f"[Gold GDELT] 90-day baseline cutoff: {cutoff_90d[:19]}")
    print(f"[Gold GDELT] 24h window cutoff:      {cutoff_24h[:19]}")

    def read_and_explode(cutoff):
        df = spark.read.format("delta").load(gkg_path) \
            .filter(F.col("ingested_at").cast("string") >= cutoff) \
            .select("ingested_at", "raw_tone_stats", "persons_array", "themes_array", "orgs_array", "locs_array") \
            .coalesce(8)
        df = df.withColumn("tone", F.split(F.col("raw_tone_stats"), ",").getItem(0).cast("double"))
        df = df.withColumn("ts", F.col("ingested_at").cast("timestamp").cast("long"))
        parts = [
            df.select("ts", "tone", F.explode("persons_array").alias("entity_name"), F.lit("person").alias("entity_type")),
            df.select("ts", "tone", F.explode("themes_array").alias("entity_name"), F.lit("theme").alias("entity_type")),
            df.select("ts", "tone", F.explode("orgs_array").alias("entity_name"), F.lit("organization").alias("entity_type")),
            df.select("ts", "tone", F.explode("locs_array").alias("entity_name"), F.lit("location").alias("entity_type")),
        ]
        result = parts[0]
        for p in parts[1:]:
            result = result.unionAll(p)
        return result.filter(F.col("entity_name").rlike("^[A-Za-z0-9 _]+$"))

    # Pass A — 90-day baseline count (GROUP BY, no window sort)
    df_90d = read_and_explode(cutoff_90d)
    df_baseline = df_90d.groupBy("entity_type", "entity_name").agg(
        F.count("ts").alias("vol_90d")
    )
    df_baseline.cache()
    df_baseline.count()  # materialise before Pass B kicks off

    # Pass B — recent 24h range windows (small data slice)
    df_24h = read_and_explode(cutoff_24h)
    win_15m = Window.partitionBy("entity_type", "entity_name").orderBy("ts").rangeBetween(-900, 0)
    win_24h = Window.partitionBy("entity_type", "entity_name").orderBy("ts").rangeBetween(-86400, 0)

    df_summary = df_24h \
        .withColumn("vol_15m",  F.count("ts").over(win_15m)) \
        .withColumn("vol_24h",  F.count("ts").over(win_24h)) \
        .withColumn("tone_15m", F.avg("tone").over(win_15m)) \
        .withColumn("tone_24h", F.avg("tone").over(win_24h))

    df_latest = df_summary.groupBy("entity_type", "entity_name").agg(
        F.max("vol_15m").alias("vol_15m"),
        F.max("vol_24h").alias("vol_24h"),
        F.last("tone_15m").alias("tone_15m"),
        F.last("tone_24h").alias("tone_24h"),
    )

    # Join baseline count back
    df_latest = df_latest.join(F.broadcast(df_baseline), on=["entity_type", "entity_name"], how="left") \
        .fillna({"vol_90d": 0})

    # 4. Spike Detection Signal (90 days baseline: 90*24*4 = 8640)
    # Require at least 10 baseline mentions before trusting the spike signal.
    # Cold-start: systems with only days of data have vol_90d ~1-5, producing 8640x+ spikes.
    # Cap at 10x so a single noisy mention can't inflate the mispricing score.
    MIN_BASELINE = 10
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
    gkg_path = os.path.join(ROOT, "data", "silver", "gdelt_gkg_history")
    events_path = os.path.join(ROOT, "data", "silver", "gdelt_events_history")
    gold_path = os.path.join(ROOT, "data", "gold", "gdelt_summaries")

    df_gold = generate_gdelt_summaries(spark, gkg_path, events_path)
    if df_gold is None:
        return

    # Cache before 3 uses — avoids recomputing the full window pipeline each time
    df_gold.cache()
    row_count = df_gold.count()  # triggers cache population

    # Save History (Append)
    history_path = gold_path + "_history"
    df_gold.write.format("delta").mode("append").option("mergeSchema", "true").save(history_path)

    # Save Current (Overwrite)
    df_gold.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(gold_path)

    df_gold.unpersist()
    print(f"[Gold GDELT] SUCCESS. Velocity signals generated for {row_count} entities.")


def main():
    print("[Gold GDELT] Initializing PySpark Session for Velocity Summaries...")
    builder = SparkSession.builder \
        .appName("PredictIQ_Gold_GDELT_Velocity") \
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
        print(f"[Gold GDELT] FATAL ERROR: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
