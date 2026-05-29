import os
import sys
import math
import shutil
from datetime import datetime, timezone, timedelta

# =====================================================================
# 1. ENVIRONMENT & PYSPARK WINDOWS INITIALIZATION
# =====================================================================
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

print("[Gold Synthesizer] Initializing Smart Mispricing Engine v2 (Sentiment-Aware)...")
builder = SparkSession.builder \
    .appName("PredictIQ_Gold_Mispricing_Scores") \
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

# Enable HTML table rendering natively inside Jupyter Notebooks
spark.conf.set("spark.sql.repl.eagerEval.enabled", True)
spark.conf.set("spark.sql.repl.eagerEval.maxNumRows", 5)

ROOT = "C:\\Data Engineering\\codeprep\\predection_project"

kalshi_history_path = os.path.join(ROOT, "data", "silver", "kalshi_markets_history")
gdelt_summaries_path = os.path.join(ROOT, "data", "gold", "gdelt_summaries")
news_summaries_path = os.path.join(ROOT, "data", "gold", "news_summaries")
gold_path = os.path.join(ROOT, "data", "gold", "mispricing_scores")

# =====================================================================
# 2. REFERENCE SETUP (REFERENCE ENGINE STRUCTURING)
# =====================================================================
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

# =====================================================================
# 3. KALSHI HISTORY DATA EXTRACTION & DATA CLEANING
# =====================================================================
lookback_cutoff_dt  = datetime.now(timezone.utc) - timedelta(hours=48)
lookback_cutoff_str = lookback_cutoff_dt.strftime("%Y-%m-%d %H:%M:%S")
df_k = spark.read.format("delta").load(kalshi_history_path)

df_k = df_k.filter(
    F.col("ticker").isNotNull() &
    F.col("ingested_at").isNotNull() &
    (F.col("ingested_at").cast("timestamp") >= F.to_timestamp(F.lit(lookback_cutoff_str)))
)
print(f"[Gold Synthesizer] 48h window filter applied. Cutoff: {lookback_cutoff_str}")

if "series_ticker" not in df_k.columns:
    df_k = df_k.withColumn("series_ticker", F.split(F.col("ticker"), "-").getItem(0))

cols_k = df_k.columns
for c in ["yes_bid", "yes_ask", "volume"]:
    if c in cols_k:
        df_k = df_k.withColumn(c, F.col(c).cast("double"))
    else:
        df_k = df_k.withColumn(c, F.lit(0.0).cast("double"))

# =====================================================================
# 4. PRICE LAG ANALYSIS AND VELOCITY MATRICES
# =====================================================================
window_spec = Window.partitionBy("ticker").orderBy("ingested_at")

df_k = df_k.withColumn("prev_yes_bid_15m", F.lag("yes_bid", 1).over(window_spec))
df_k = df_k.withColumn("prev_yes_bid_1h", F.lag("yes_bid", 4).over(window_spec))
df_k = df_k.withColumn("prev_yes_bid_24h", F.lag("yes_bid", 96).over(window_spec))

df_k = df_k.withColumn(
    "delta_15m", 
    F.when(F.col("prev_yes_bid_15m") > 0, (F.col("yes_bid") - F.col("prev_yes_bid_15m")) / F.col("prev_yes_bid_15m")).otherwise(F.lit(0.0))
).withColumn(
    "delta_1h", 
    F.when(F.col("prev_yes_bid_1h") > 0, (F.col("yes_bid") - F.col("prev_yes_bid_1h")) / F.col("prev_yes_bid_1h")).otherwise(F.lit(0.0))
).withColumn(
    "delta_24h", 
    F.when(F.col("prev_yes_bid_24h") > 0, (F.col("yes_bid") - F.col("prev_yes_bid_24h")) / F.col("prev_yes_bid_24h")).otherwise(F.lit(0.0))
)

df_with_deltas = df_k

# =====================================================================
# 5. MARKET PROFILE STATE ISOLATION (LATEST ACTIVE ROWS)
# =====================================================================
window_latest = Window.partitionBy("ticker").orderBy(F.col("ingested_at").desc())
df_latest = df_with_deltas.withColumn("rn", F.row_number().over(window_latest)).filter(F.col("rn") == 1).drop("rn")

if "status" in df_latest.columns:
    df_latest = df_latest.filter(F.col("status").isin(["active", "open"]))

# =====================================================================
# 6. EXTERNAL ALTERNATIVE DATA EXTRACTION (GDELT & VADER NEWS)
# =====================================================================
if not os.path.exists(os.path.join(gdelt_summaries_path, "_delta_log")):
    print("[Gold Synthesizer] GDELT Summaries not found.")
    df_g = None
else:
    df_g = spark.read.format("delta").load(gdelt_summaries_path) \
        .withColumn("norm_sentiment", F.least(F.lit(1.0), F.greatest(F.lit(-1.0), F.col("tone_15m") / 10.0)))

if not os.path.exists(os.path.join(news_summaries_path, "_delta_log")):
    print("[Gold Synthesizer] News Summaries not found.")
    df_n = None
else:
    df_n = spark.read.format("delta").load(news_summaries_path) \
        .withColumn("norm_sentiment", F.col("tone_15m"))

df_map = spark.read.csv(mapping_file, header=True)

# =====================================================================
# 7. ROUTING PIPELINE ENGINE: A. GDELT THEME ROUTING
# =====================================================================
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

# =====================================================================
# 8. ROUTING PIPELINE ENGINE: B. GDELT PERSON STRUCTURAL INTERSECT
# =====================================================================
if df_g:
    df_g_persons = df_g.filter(F.col("entity_type") == "person").select("entity_name", "vol_spike_multiplier", "norm_sentiment")
    df_kalshi_titles = df_latest.select("ticker", "title")
    df_joined_persons = df_kalshi_titles.crossJoin(df_g_persons) \
        .filter(F.expr("locate(entity_name, title) > 0")) \
        .select("ticker", "vol_spike_multiplier", "norm_sentiment")
else:
    df_joined_persons = spark.createDataFrame([], "ticker string, vol_spike_multiplier double, norm_sentiment double")

# =====================================================================
# 9. ROUTING PIPELINE ENGINE: C. FREE-TEXT NEWS KEYWORD PIPELINE
# =====================================================================
silver_news_path = os.path.join(ROOT, "data", "silver", "news_articles_enriched")

if df_n is not None and os.path.exists(os.path.join(silver_news_path, "_delta_log")):
    cutoff_24h = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
    
    STOPWORDS = {"will", "the", "this", "that", "with", "from", "have", "been", "than", "more", "about", "what", "when", "where", "which", "their", "they", "would", "could", "should", "after", "before", "into", "over", "make", "some", "just", "like", "then", "also", "only", "says", "said", "week", "next", "last", "year", "amid", "ahead"}

    df_silver_articles = spark.read.format("delta").load(silver_news_path) \
        .filter(F.col("ingested_at").cast("string") >= cutoff_24h) \
        .filter(F.col("title").isNotNull()) \
        .select("feed_key", F.col("title").alias("article_title"))
        
    df_source_words = df_silver_articles \
        .withColumn("clean_title", F.lower(F.regexp_replace(F.col("article_title"), "[^a-zA-Z0-9 ]", ""))) \
        .withColumn("word", F.explode(F.split(F.col("clean_title"), " "))) \
        .filter(F.length(F.col("word")) > 3) \
        .filter(~F.col("word").isin(list(STOPWORDS))) \
        .select("feed_key", "word") \
        .distinct()
        
    df_kalshi_titles_lower = df_latest.select("ticker", F.lower(F.col("title")).alias("market_title_lower"))
    
    df_news_keyword_matches = df_source_words \
        .crossJoin(F.broadcast(df_kalshi_titles_lower)) \
        .filter(F.col("market_title_lower").contains(F.col("word"))) \
        .select("ticker", "feed_key", F.col("word").alias("keyword")) \
        .distinct()
        
    df_joined_news = df_news_keyword_matches \
        .join(
            df_n.select("feed_key", "keyword", "vol_spike_multiplier", F.col("tone_15m").alias("norm_sentiment")),
            on=["feed_key", "keyword"],
            how="inner"
        ) \
        .select("ticker", "vol_spike_multiplier", "norm_sentiment")
    print(f"[Gold Synthesizer] News keyword routing: {df_news_keyword_matches.count()} (ticker, feed) matches found.")
else:
    df_joined_news = spark.createDataFrame([], "ticker string, vol_spike_multiplier double, norm_sentiment double")

# =====================================================================
# 10. MATRIX SIGNAL COMPILATION & PRIMARY SOURCE ANALYSIS
# =====================================================================
df_all_matches = df_joined_themes.unionAll(df_joined_persons).unionAll(df_joined_news)

win_top_spike = Window.partitionBy("ticker").orderBy(F.col("vol_spike_multiplier").desc())

df_max_signal = df_all_matches \
    .withColumn("_rn", F.row_number().over(win_top_spike)) \
    .filter(F.col("_rn") == 1) \
    .drop("_rn") \
    .select(
        "ticker",
        F.col("vol_spike_multiplier").alias("max_spike_multiplier"),
        F.col("norm_sentiment").alias("avg_sentiment"),
    )

df_final = df_latest.join(df_max_signal, on="ticker", how="left")

# =====================================================================
# 11. QUANT ENGINE: LOG-ANCHORED ARBITRAGE MARKERS (V2 SCORING)
# =====================================================================
V2_TOP5_ANCHOR = float(os.getenv("MISPRICING_V2_ANCHOR", "306.0"))
ln_anchor      = math.log1p(V2_TOP5_ANCHOR)

raw_v2 = (
    F.when(F.col("max_spike_multiplier").isNull(), F.lit(0.0))
     .otherwise(F.col("max_spike_multiplier") * F.abs(F.col("avg_sentiment")) * (F.lit(1.0) + F.abs(F.col("delta_24h"))))
)
df_final = df_final.withColumn("raw_score", raw_v2)

df_final = df_final.withColumn(
    "mispricing_score",
    F.least(
        F.lit(100.0),
        F.greatest(
            F.lit(0.0),
            F.lit(80.0) * F.log1p(F.col("raw_score")) / F.lit(ln_anchor)
        )
    )
).drop("raw_score")

df_final = df_final.withColumn("flagged_candidate", (F.col("mispricing_score") > 65.0)) \
                   .withColumn("llm_brief", F.lit(None).cast("string"))

cols_f = df_final.columns
df_gold = df_final.select(
    "ticker",
    F.col("title") if "title" in cols_f else F.lit(None).cast("string").alias("title"),
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

# =====================================================================
# 12. DISK WRITING PERSISTENCE MANAGEMENT (DELTA LAKE INGEST)
# =====================================================================
df_gold.cache()
row_count = df_gold.count()
print(f"[Gold Synthesizer] Computed {row_count} Market Scores. Writing to Delta...")

history_path = gold_path + "_history"

if os.path.exists(gold_path):
    shutil.rmtree(gold_path)
    print(f"[Gold Synthesizer] Cleared old Delta at {gold_path}")

df_gold.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(gold_path)

print(f"[Gold Synthesizer] Appending to History Ledger at {history_path}")
df_gold.write.format("delta").mode("append").option("mergeSchema", "true").save(history_path)

df_gold.unpersist()
print("[Gold Synthesizer] SUCCESS. Smart Scoring Ledger updated.")

# Stop active session environment
spark.stop()