from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import os

# Milan's Machine PySpark Config
spark = SparkSession.builder \
    .appName("PredictIQ_Silver_GDELT_Events") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Pathing
PROJECT_ROOT = "."
BRONZE_PATH  = os.path.join(PROJECT_ROOT, "data", "bronze", "gdelt", "gdelt_events", "*.parquet")
REFERENCE_DIR = os.path.join(PROJECT_ROOT, "reference")
SILVER_PATH = os.path.join(PROJECT_ROOT, "data/silver/gdelt_events_enriched")

print(f"📡 Reading GDELT Events from: {BRONZE_PATH}")
df = spark.read.parquet(BRONZE_PATH)

# Enriched Join Logic
joins = [
    ("cameo_eventcodes.csv", "EventCode", "EventCode_Description"),
    ("cameo_quadclass.csv", "QuadClass", "QuadClass_Description"),
    ("cameo_actortypes.csv", "Actor1Type", "Actor1Type_Description"),
    ("cameo_actortypes.csv", "Actor2Type", "Actor2Type_Description")
]

for filename, join_key, desc_col in joins:
    filepath = os.path.join(REFERENCE_DIR, filename)
    if os.path.exists(filepath):
        print(f"📋 Joining {filename} on {join_key}...")
        ref_df = spark.read.csv(filepath, header=True, inferSchema=True)
        # Rename description column if filename shared (like ActorType)
        ref_df = ref_df.withColumnRenamed("Description", desc_col)
        # Key column also needs to match
        ref_df = ref_df.withColumnRenamed("Code", join_key)
        df = df.join(ref_df, on=join_key, how="left")
    else:
        print(f"⚠️ {filename} missing! Skipping {desc_col} enrichment.")

# Join Countries (multi-join)
countries_file = os.path.join(REFERENCE_DIR, "fips_countries.csv")
if os.path.exists(countries_file):
    print("📋 Joining FIPS Country Codes...")
    country_df = spark.read.csv(countries_file, header=True, inferSchema=True)
    
    # Actor 1
    df = df.join(country_df.withColumnRenamed("Name", "Actor1Country")
               .withColumnRenamed("Code", "Actor1CountryCode"),
               F.col("Actor1CountryCode") == F.col("Actor1CountryCode"), "left").drop("Actor1CountryCode")
    
    # Actor 2
    df = df.join(country_df.withColumnRenamed("Name", "Actor2Country")
               .withColumnRenamed("Code", "Actor2CountryCode"),
               F.col("Actor2CountryCode") == F.col("Actor2CountryCode"), "left").drop("Actor2CountryCode")
    
    # Action Country
    df = df.join(country_df.withColumnRenamed("Name", "ActionCountry")
               .withColumnRenamed("Code", "ActionCountryCode"),
               F.col("ActionCountryCode") == F.col("ActionCountryCode"), "left").drop("ActionCountryCode")
else:
    print("⚠️ fips_countries.csv missing!")

print(f"💾 Writing Enriched Events to: {SILVER_PATH} (Append)")
df.write.format("delta").mode("append").save(SILVER_PATH)

print("✅ GDELT Events Silver transformation complete.")
spark.stop()
