import os
import sys
from datetime import datetime, timezone, timedelta

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
    """
    Creates the reference mapping file ONLY if it does not exist.
    IMPORTANT: Never overwrites an existing file — protects custom regex rules.
    """
    ref_dir = os.path.join(ROOT, "data", "reference")
    os.makedirs(ref_dir, exist_ok=True)
    mapping_file = os.path.join(ref_dir, "kalshi_theme_map.csv")
    if not os.path.exists(mapping_file):
        print("[Gold Synthesizer] kalshi_theme_map.csv not found. Creating default stub...")
        with open(mapping_file, "w") as f:
            f.write("series_ticker,gdelt_entity_type,gdelt_entity_name,required_regex\n")
            f.write("KXFED,organization,Federal Reserve,(Federal Reserve|FOMC|Jerome Powell|Fed Chair)\n")
            f.write("KXFED,theme,CENTRAL_BANK,(Federal Reserve|Federal Open Market|FOMC|Jerome Powell)\n")
            f.write("KXPOLITICS,theme,ELECTION,(U\\.S\\. Election|American Election|Senate Election|Midterm)\n")
            f.write("KXTW,location,Taiwan,(Taiwan|Taipei|ROC |TSMC|Cross-Strait)\n")
        print(f"[Gold Synthesizer] Default stub written to {mapping_file}")
    else:
        print(f"[Gold Synthesizer] Using existing kalshi_theme_map.csv at {mapping_file}")
    return mapping_file

def generate_mispricing_scores(spark, kalshi_history_path, gdelt_summaries_path, news_summaries_path, mapping_file):
    # 1. Load Kalshi Current State with Velocity Deltas
    # Use a 48-hour sliding window — enough for velocity math, prevents full-history scan
    lookback_cutoff_dt  = datetime.now(timezone.utc) - timedelta(hours=48)
    lookback_cutoff_str = lookback_cutoff_dt.strftime("%Y-%m-%d %H:%M:%S")
    df_k = spark.read.format("delta").load(kalshi_history_path)
    if "ticker" not in df_k.columns or "ingested_at" not in df_k.columns:
        return None
    # Cast to timestamp for proper comparison — string comparison fails because PySpark
    # formats timestamps as "YYYY-MM-DD HH:MM:SS" (space) while Python isoformat uses "T"
    df_k = df_k.filter(
        F.col("ticker").isNotNull() &
        F.col("ingested_at").isNotNull() &
        (F.col("ingested_at").cast("timestamp") >= F.to_timestamp(F.lit(lookback_cutoff_str)))
    )
    print(f"[Gold Synthesizer] 48h window filter applied. Cutoff: {lookback_cutoff_str}")

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
        print("[Gold Synthesizer] GDELT Summaries not found.")
        df_g = None
    else:
        # Normalize GDELT Tone (-10/+10) to -1/+1 scale
        df_g = spark.read.format("delta").load(gdelt_summaries_path) \
            .withColumn("norm_sentiment", F.least(F.lit(1.0), F.greatest(F.lit(-1.0), F.col("tone_15m") / 10.0)))

    # 3. Load News Spikes (Current)
    if not os.path.exists(os.path.join(news_summaries_path, "_delta_log")):
        print("[Gold Synthesizer] News Summaries not found.")
        df_n = None
    else:
        # VADER is already -1/+1
        df_n = spark.read.format("delta").load(news_summaries_path) \
            .withColumn("norm_sentiment", F.col("sent_15m"))

    # 4. Apply Bridge / Mapping
    df_map = spark.read.csv(mapping_file, header=True)
    
    # --- A. GDELT Theme Joins ---
    if df_g:
        df_joined_themes = df_latest.join(df_map, on="series_ticker", how="inner") \
            .join(
                df_g,
                (F.col("gdelt_entity_type") == F.col("entity_type")) &
                (
                    F.lower(F.col("entity_name")).contains(F.lower(F.col("gdelt_entity_name"))) |
                    F.lower(F.col("gdelt_entity_name")).contains(F.lower(F.col("entity_name")))
                ),
                how="inner"
            ) \
            .select("ticker", "vol_spike_multiplier", "norm_sentiment")
    else:
        df_joined_themes = spark.createDataFrame([], "ticker string, vol_spike_multiplier double, norm_sentiment double")

    # --- B. GDELT Person Joins ---
    if df_g:
        df_g_persons = df_g.filter(F.col("entity_type") == "person").select("entity_name", "vol_spike_multiplier", "norm_sentiment")
        df_kalshi_titles = df_latest.select("ticker", "title")
        df_joined_persons = df_kalshi_titles.crossJoin(df_g_persons) \
            .filter(F.expr("locate(entity_name, title) > 0")) \
            .select("ticker", "vol_spike_multiplier", "norm_sentiment")
    else:
        df_joined_persons = spark.createDataFrame([], "ticker string, vol_spike_multiplier double, norm_sentiment double")

    # --- C. News Source Joins (Broad signal fallback) ---
    # Note: News is source-level, so we only join if the market series matches a hypothetical 'news_topic' 
    # For now, we union any mapped news signal if available
    df_joined_news = spark.createDataFrame([], "ticker string, vol_spike_multiplier double, norm_sentiment double")
    # (Future: add source-to-series mapping if needed)

    # Combine All Signals
    df_all_matches = df_joined_themes.unionAll(df_joined_persons).unionAll(df_joined_news)

    # Aggregate to find the Best Opportunity per Market (Max weighted signal)
    # We want the highest vol_spike but also consider the sentiment strength
    df_max_signal = df_all_matches.groupBy("ticker").agg(
        F.max("vol_spike_multiplier").alias("max_spike_multiplier"),
        F.last("norm_sentiment").alias("avg_sentiment") # Use the sentiment associated with the entity
    )

    # Join back to Market Data
    df_final = df_latest.join(df_max_signal, on="ticker", how="left")

    # 5. Calculate Smart Mispricing Score
    # Formula: (VolSpike * abs(Sentiment) * 20) * (1 - abs(PriceDelta))
    # This rewards high news signal + strong sentiment but DECAYS if the price already moved.
    
    # Score = spike(0-10) × |sentiment|(0-1) × 20 × (1 - |delta|)
    # Max raw = 10 × 1.0 × 20 = 200, capped at 100.
    # Threshold of 80 requires spike ≥ 8x + sentiment ≥ 0.5, or spike ≥ 4x + sentiment ≥ 1.0.
    smart_score = (
        F.when(F.col("max_spike_multiplier").isNull(), F.lit(0.0))
         .otherwise(
             (F.col("max_spike_multiplier") * F.abs(F.col("avg_sentiment")) * 20.0) *
             (F.lit(1.0) - F.abs(F.col("delta_15m")))
         )
    )

    df_final = df_final.withColumn("raw_score", smart_score)
    
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
        "llm_brief", F.lit(None).cast("string")
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
        F.col("avg_sentiment").alias("sentiment_signal"),
        "mispricing_score",
        "flagged_candidate",
        "llm_brief",
        F.current_timestamp().alias("ingested_at")
    )

    return df_gold

def main():
    print("[Gold Synthesizer] Initializing Smart Mispricing Engine v2 (Sentiment-Aware)...")
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
    news_summaries = os.path.join(ROOT, "data", "gold", "news_summaries")
    gold_path = os.path.join(ROOT, "data", "gold", "mispricing_scores")
    
    mapping_file = setup_dummy_mapping_if_missing()

    try:
        df_gold = generate_mispricing_scores(spark, kalshi_history, gdelt_summaries, news_summaries, mapping_file)
        if df_gold is None:
            return
            
        import shutil
        row_count = df_gold.count()
        print(f"[Gold Synthesizer] Computed {row_count} Market Scores. Writing to Delta...")

        history_path = gold_path + "_history"
        
        # Only wipe the Current snapshot so it starts clean at version 000
        if os.path.exists(gold_path):
            shutil.rmtree(gold_path)
            print(f"[Gold Synthesizer] Cleared old Delta at {gold_path}")

        # Save Current snapshot (fresh write — always version 000)
        df_gold.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(gold_path)

        # Save History ledger (Append-only)
        print(f"[Gold Synthesizer] Appending to History Ledger at {history_path}")
        df_gold.write.format("delta").mode("append").option("mergeSchema", "true").save(history_path)
        
        print("[Gold Synthesizer] SUCCESS. Smart Scoring Ledger updated.")
        
    except Exception as e:
        print(f"[Gold Synthesizer] FATAL ERROR: {str(e)}")
        raise e
    finally:
        spark.stop()

if __name__ == "__main__":
    main()


if __name__ == "__main__":
    main()
