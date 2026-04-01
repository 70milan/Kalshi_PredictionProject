from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType, StringType
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import os

# Milan's Machine PySpark Config
spark = SparkSession.builder \
    .appName("PredictIQ_Silver_News") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Sentiment Analysis UDF
analyzer = SentimentIntensityAnalyzer()

def get_sentiment_score(title, summary):
    text = f"{title or ''} {summary or ''}"
    return float(analyzer.polarity_scores(text)['compound'])

def get_sentiment_label(score):
    if score >= 0.05: return 'Positive'
    elif score <= -0.05: return 'Negative'
    else: return 'Neutral'

sentiment_score_udf = F.udf(get_sentiment_score, DoubleType())
sentiment_label_udf = F.udf(get_sentiment_label, StringType())

# Pathing
PROJECT_ROOT = "."
NYT_BRONZE_PATH = os.path.join(PROJECT_ROOT, "data/bronze/nyt/*.parquet")
REUTERS_BRONZE_PATH = os.path.join(PROJECT_ROOT, "data/bronze/reuters/*.parquet")
SILVER_PATH = os.path.join(PROJECT_ROOT, "data/silver/news_articles_enriched")

print("📡 Loading News Bronze Data...")
nyt_df = None
if os.path.exists(os.path.dirname(NYT_BRONZE_PATH)):
    nyt_df = spark.read.parquet(NYT_BRONZE_PATH).withColumn("source", F.lit("NYT"))

reuters_df = None
if os.path.exists(os.path.dirname(REUTERS_BRONZE_PATH)):
    reuters_df = spark.read.parquet(REUTERS_BRONZE_PATH).withColumn("source", F.lit("Reuters"))

# Union if both exist
if nyt_df and reuters_df:
    # Ensure matching columns (may need selection/alignment)
    df = nyt_df.unionByName(reuters_df, allowMissingColumns=True)
elif nyt_df:
    df = nyt_df
elif reuters_df:
    df = reuters_df
else:
    print("❌ No news data found!")
    spark.stop()
    exit()

# Enrichment
print("✨ Standardizing timestamps and calculating sentiment...")
df = df.withColumn("published_at", F.to_timestamp("pubDate")) # Adjust based on schema

df = df.withColumn("sentiment_score", sentiment_score_udf(F.col("title"), F.col("summary")))
df = df.withColumn("sentiment_label", sentiment_label_udf(F.col("sentiment_score")))

print(f"💾 Writing Enriched News to: {SILVER_PATH} (Append)")
df.write.format("delta").mode("append").save(SILVER_PATH)

print("✅ News Silver transformation complete.")
spark.stop()
