import os
import sys
from datetime import datetime, timedelta

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

def generate_gdelt_summaries(spark, gkg_path, events_path):
    log_gkg = os.path.exists(os.path.join(gkg_path, "_delta_log"))
    log_events = os.path.exists(os.path.join(events_path, "_delta_log"))
    
    if not (log_gkg or log_events):
        print("[Gold GDELT Summaries] Silver history tables do not exist yet. Exiting.")
        return None

    # Calculate 7-day cutoff Date (Timestamp arithmetic)
    cutoff = F.current_timestamp() - F.expr("INTERVAL 7 DAYS")

    mentions_dfs = []

    # 1. Process GKG History
    if log_gkg:
        df_gkg = spark.read.format("delta").load(gkg_path)
        
        # Filter for last 7 days
        df_gkg_7d = df_gkg.filter(
            (F.col("ingested_at").isNotNull()) & 
            (F.to_timestamp(F.col("ingested_at")) > cutoff)
        )
        
        # Extract average tone (first value in raw_tone_stats)
        if "raw_tone_stats" in df_gkg_7d.columns:
            df_gkg_7d = df_gkg_7d.withColumn("tone", F.split(F.col("raw_tone_stats"), ",").getItem(0).cast("double"))
        else:
            df_gkg_7d = df_gkg_7d.withColumn("tone", F.lit(0.0).cast("double"))

        # Explode Persons
        if "persons_array" in df_gkg_7d.columns:
            gkg_persons = df_gkg_7d.select(
                F.explode("persons_array").alias("entity_name"),
                F.col("tone"),
                F.lit("person").alias("entity_type"),
                F.lit(1.0).alias("mentions")  # 1 Doc = 1 base mention for GKG
            ).filter(F.col("entity_name").rlike("^[A-Za-z0-9 ]+$"))  # Alpha numeric names only
            mentions_dfs.append(gkg_persons)

        # Explode Themes
        if "themes_array" in df_gkg_7d.columns:
            gkg_themes = df_gkg_7d.select(
                F.explode("themes_array").alias("entity_name"),
                F.col("tone"),
                F.lit("theme").alias("entity_type"),
                F.lit(1.0).alias("mentions")
            ).filter(F.col("entity_name").rlike("^[A-Z0-9_]+$")) # Themes are ALL CAPS with underscores
            mentions_dfs.append(gkg_themes)

    # 2. Process Events History
    if log_events:
        df_events = spark.read.format("delta").load(events_path)
        
        df_events_7d = df_events.filter(
            (F.col("ingested_at").isNotNull()) & 
            (F.to_timestamp(F.col("ingested_at")) > cutoff)
        )

        cols = df_events_7d.columns
        if "actor1_name" in cols and "tone_score" in cols:
            events_actor1 = df_events_7d.filter(F.col("actor1_name").isNotNull()).select(
                F.col("actor1_name").alias("entity_name"),
                F.col("tone_score").alias("tone"),
                F.lit("person").alias("entity_type"),
                F.coalesce(F.col("mention_count").cast("double"), F.lit(1.0)).alias("mentions")
            )
            mentions_dfs.append(events_actor1)

    if not mentions_dfs:
        print("[Gold GDELT Summaries] No recent data found.")
        return None

    # Union all slices
    all_mentions = mentions_dfs[0]
    for df in mentions_dfs[1:]:
        all_mentions = all_mentions.unionAll(df)

    # 3. Aggregate
    # Calculate total mentions and unweighted average tone
    df_grouped = all_mentions.groupBy("entity_type", "entity_name").agg(
        F.sum("mentions").alias("rolling_7d_mentions"),
        F.avg("tone").alias("rolling_7d_tone")
    )

    # V4 Staleness Guard: Mark table refresh timestamp
    df_gold = df_grouped.withColumn("ingested_at", F.current_timestamp())
    
    # Optional: Filter out trivial noise (entities with tiny mention counts over 7 days)
    df_gold = df_gold.filter(F.col("rolling_7d_mentions") > 2)

    return df_gold

def main():
    print("[Gold GDELT] Initializing PySpark Session for Ground Truth Summaries...")
    ivy_dir = os.environ.get("IVY_PACKAGE_DIR", "")
    builder = SparkSession.builder \
        .appName("PredictIQ_Gold_GDELT_Summaries") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.parquet.enableVectorizedReader", "false")
    if ivy_dir:
        builder = builder.config("spark.jars.ivy", ivy_dir)

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    gkg_path = os.path.join(ROOT, "data", "silver", "gdelt_gkg_history")
    events_path = os.path.join(ROOT, "data", "silver", "gdelt_events_history")
    gold_path = os.path.join(ROOT, "data", "gold", "gdelt_summaries")

    try:
        df_gold = generate_gdelt_summaries(spark, gkg_path, events_path)
        if df_gold is None:
            return
            
        row_count = df_gold.count()
        print(f"[Gold GDELT] Generated {row_count} Entity Aggregates. Writing to Delta...")
        
        # Overwrite mode: always present the latest 7-day snapshot
        df_gold.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(gold_path)
        print("[Gold GDELT] SUCCESS. Ground Truth Summaries table completely updated.")
        
    except Exception as e:
        print(f"[Gold GDELT] FATAL ERROR: {str(e)}")
        raise e
    finally:
        spark.stop()

if __name__ == "__main__":
    import time
    import traceback

    print("[Gold GDELT] Docker Polling Service Initialized (5-min intervals).")
    while True:
        try:
            main()
            print("[Gold GDELT] Run complete. Sleeping for 300 seconds...")
        except Exception:
            print("[Gold GDELT] LOOP ERROR detected:")
            traceback.print_exc()
            print("[Gold GDELT] Sleeping for 300 seconds before retry...")
        
        time.sleep(300)
