import os
import sys
from datetime import datetime, timezone

# PySpark Windows Fixes
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

_hadoop_home = os.environ.get("HADOOP_HOME", "")
if _hadoop_home:
    os.environ["PATH"] = os.path.join(_hadoop_home, "bin") + ";" + os.environ.get("PATH", "")

os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from pyspark.sql.window import Window
from delta import configure_spark_with_delta_pip

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def calculate_odds_deltas(df):
    """
    Calculates velocity metrics over 15-minute, 1-hour, and 24-hour windows.
    """
    cols = df.columns
    for c in ["yes_bid", "yes_ask", "volume"]:
        if c in cols:
            df = df.withColumn(c, F.col(c).cast("double"))
        else:
            df = df.withColumn(c, F.lit(0.0).cast("double"))

    window_spec = Window.partitionBy("ticker").orderBy("ingested_at")
    
    # Use lag exactly by 1 (15m), 4 (1h), and 96 (24h)
    df = df.withColumn("prev_yes_bid_15m", F.lag("yes_bid", 1).over(window_spec))
    df = df.withColumn("prev_yes_bid_1h", F.lag("yes_bid", 4).over(window_spec))
    df = df.withColumn("prev_yes_bid_24h", F.lag("yes_bid", 96).over(window_spec))
    
    # Percentage deltas
    df = df.withColumn(
        "delta_15m", 
        F.when(F.col("prev_yes_bid_15m") > 0, (F.col("yes_bid") - F.col("prev_yes_bid_15m")) / F.col("prev_yes_bid_15m")).otherwise(F.lit(0.0))
    ).withColumn(
        "delta_1h", 
        F.when(F.col("prev_yes_bid_1h") > 0, (F.col("yes_bid") - F.col("prev_yes_bid_1h")) / F.col("prev_yes_bid_1h")).otherwise(F.lit(0.0))
    ).withColumn(
        "delta_24h", 
        F.when(F.col("prev_yes_bid_24h") > 0, (F.col("yes_bid") - F.col("prev_yes_bid_24h")) / F.col("prev_yes_bid_24h")).otherwise(F.lit(0.0))
    )
    return df

def setup_dummy_mapping_if_missing():
    """Creates a dummy reference mapping for Themes/Orgs/Locations if it doesn't exist."""
    ref_dir = os.path.join(ROOT, "data", "reference")
    os.makedirs(ref_dir, exist_ok=True)
    mapping_file = os.path.join(ref_dir, "kalshi_theme_map.csv")
    if not os.path.exists(mapping_file):
        with open(mapping_file, "w") as f:
            f.write("series_ticker,gdelt_entity_type,gdelt_entity_name\n")
            f.write("KXFED,organization,Federal Reserve\n")
            f.write("KXFED,theme,CENTRAL_BANK\n")
            f.write("KXPOLITICS,theme,ELECTION\n")
            f.write("KXTW,location,Taiwan\n")
    return mapping_file

def generate_mispricing_scores(spark, kalshi_history_path, gdelt_summaries_path, mapping_file):
    # 1. Load Kalshi Current State with Velocity Deltas
    df_k = spark.read.format("delta").load(kalshi_history_path)
    if "ticker" not in df_k.columns or "ingested_at" not in df_k.columns:
        return None
    df_k = df_k.filter(F.col("ticker").isNotNull() & F.col("ingested_at").isNotNull())

    # Ensure series_ticker exists (fallback to prefix if missing)
    if "series_ticker" not in df_k.columns:
        df_k = df_k.withColumn("series_ticker", F.split(F.col("ticker"), "-").getItem(0))

    df_with_deltas = calculate_odds_deltas(df_k)
    window_latest = Window.partitionBy("ticker").orderBy(F.col("ingested_at").desc())
    df_latest = df_with_deltas.withColumn("rn", F.row_number().over(window_latest)).filter(F.col("rn") == 1).drop("rn")

    # Filter out finalized/dead markets
    if "status" in df_latest.columns:
        df_latest = df_latest.filter(F.col("status").isin(["active", "open"]))

    # 2. Load GDELT Spikes (Current)
    if not os.path.exists(os.path.join(gdelt_summaries_path, "_delta_log")):
        print("[Gold Synthesizer] GDELT Summaries not found. Exiting.")
        return None
    df_g = spark.read.format("delta").load(gdelt_summaries_path)

    # 3. Apply Bridge / Mapping
    # Load Theme Mapping (Option B)
    df_map = spark.read.csv(mapping_file, header=True)
    
    # Join Kalshi to Mapping, then to GDELT
    df_joined_themes = df_latest.join(df_map, on="series_ticker", how="inner") \
        .join(
            df_g, 
            (F.col("gdelt_entity_type") == F.col("entity_type")) & 
            (F.col("gdelt_entity_name") == F.col("entity_name")), 
            how="inner"
        ) \
        .select("ticker", "vol_spike_multiplier")

    # Option A: Regex / Text Match for Persons
    # Cross join kalshi titles with GDELT 'person' entities (cheap because records are small)
    df_g_persons = df_g.filter(F.col("entity_type") == "person").select("entity_name", "vol_spike_multiplier")
    df_kalshi_titles = df_latest.select("ticker", "title")
    df_joined_persons = df_kalshi_titles.crossJoin(df_g_persons) \
        .filter(F.expr("locate(entity_name, title) > 0")) \
        .select("ticker", "vol_spike_multiplier")

    # Combine Matches
    df_all_matches = df_joined_themes.unionAll(df_joined_persons)

    # Aggregate to find the Max Spike per Market
    df_max_spike = df_all_matches.groupBy("ticker").agg(
        F.max("vol_spike_multiplier").alias("max_spike_multiplier")
    )

    # Join back to Market Data
    df_final = df_latest.join(df_max_spike, on="ticker", how="left")

    # 4. Calculate Mispricing Score
    # Option A Strictness: If no mapped entities, score is 0.
    # Score Formula: (Vol Spike) - (Price Delta % * 100).
    # Example: 5x volume spike, 10% price delta -> 5.0 - (0.10 * 100) = 5.0 - 10 = -5 (Priced in)
    # Example: 10x volume spike, 2% price delta -> 10.0 - (0.02 * 100) = 10.0 - 2.0 = 8.0 -> Multiplied to 0-100 scale.
    
    calculate_score = (
        F.when(F.col("max_spike_multiplier").isNull(), F.lit(0.0))
         .otherwise(
             (F.col("max_spike_multiplier") * 10.0) - (F.col("delta_15m") * 100.0)
         )
    )

    df_final = df_final.withColumn("raw_score", calculate_score)
    
    # Cap between 0 and 100
    df_final = df_final.withColumn(
        "mispricing_score",
        F.when(F.col("raw_score") < 0, F.lit(0.0))
         .when(F.col("raw_score") > 100, F.lit(100.0))
         .otherwise(F.col("raw_score"))
    ).drop("raw_score")

    # Flag candidates > 80
    df_final = df_final.withColumn(
        "flagged_candidate",
        (F.col("mispricing_score") >= 80.0)
    ).withColumn(
        "llm_brief", F.lit(None).cast("string") # Phase 4 Placeholder
    )

    # Format cleanly
    cols = df_final.columns
    df_gold = df_final.select(
        "ticker",
        F.col("title") if "title" in cols else F.lit(None).cast("string").alias("title"),
        "yes_bid",
        "delta_15m",
        "delta_1h",
        "delta_24h",
        "max_spike_multiplier",
        "mispricing_score",
        "flagged_candidate",
        "llm_brief",
        F.current_timestamp().alias("ingested_at")
    )

    return df_gold

def main():
    print("[Gold Synthesizer] Initializing Mispricing Engine...")
    builder = SparkSession.builder \
        .appName("PredictIQ_Gold_Mispricing_Scores") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.parquet.enableVectorizedReader", "false")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    kalshi_history = os.path.join(ROOT, "data", "silver", "kalshi_markets_history")
    gdelt_summaries = os.path.join(ROOT, "data", "gold", "gdelt_summaries")
    gold_path = os.path.join(ROOT, "data", "gold", "mispricing_scores")
    
    mapping_file = setup_dummy_mapping_if_missing()

    try:
        df_gold = generate_mispricing_scores(spark, kalshi_history, gdelt_summaries, mapping_file)
        if df_gold is None:
            return
            
        row_count = df_gold.count()
        print(f"[Gold Synthesizer] Computed {row_count} Market Scores. Writing to Delta...")
        
        # Save History (Append-only Ledger)
        history_path = gold_path + "_history"
        df_gold.write.format("delta").mode("append").option("mergeSchema", "true").save(history_path)

        # Save Current (Overwrite UI Snapshot)
        df_gold.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(gold_path)
        
        print("[Gold Synthesizer] SUCCESS. Scoring Ledger updated.")
        
    except Exception as e:
        print(f"[Gold Synthesizer] FATAL ERROR: {str(e)}")
        raise e
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
