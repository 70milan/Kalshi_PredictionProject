from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import os

# Milan's Machine PySpark Config
spark = SparkSession.builder \
    .appName("PredictIQ_Silver_GDELT_GKG") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Pathing
PROJECT_ROOT = "."
BRONZE_PATH  = os.path.join(PROJECT_ROOT, "data", "bronze", "gdelt", "gdelt_gkg", "*.parquet")
REFERENCE_DIR = os.path.join(PROJECT_ROOT, "reference")
SILVER_PATH = os.path.join(PROJECT_ROOT, "data/silver/gdelt_gkg_enriched")

print(f"📡 Reading GDELT GKG from: {BRONZE_PATH}")
df = spark.read.parquet(BRONZE_PATH)

# Enriched Transformation Logic
print("✨ Cleaning Persons, Themes, and Organizations into arrays...")

# Persons are often format: Name,Offset;Name,Offset
# Themes are often: Theme;Theme
# Organizations are: Org,Offset;Org,Offset
# We want to extract the Names/Themes/Orgs without offsets.

def extract_names(raw):
    if not raw: return []
    items = raw.split(";")
    # Extract only text part (before comma)
    return [item.split(",")[0] for item in items if item]

extract_udf = F.udf(extract_names, "array<string>")

df = df.withColumn("persons", extract_udf(F.col("V2Persons")))
df = df.withColumn("themes", extract_udf(F.col("V2Themes")))
df = df.withColumn("organizations", extract_udf(F.col("V2Organizations")))

# Join Countries (V2Locations often contains country codes like US, UK, etc.)
countries_file = os.path.join(REFERENCE_DIR, "fips_countries.csv")
if os.path.exists(countries_file):
    print("📋 Joining FIPS Country Codes...")
    country_df = spark.read.csv(countries_file, header=True, inferSchema=True)
    
    # Simple join assuming Actor1CountryCode logic?
    # GKG has Locations field: Type#Name#CC#etc.
    # For now, let's keep it simple as per plan.
    df = df.join(country_df.withColumnRenamed("Name", "PrimaryCountryName")
               .withColumnRenamed("Code", "PrimaryCountryCode"),
               F.col("PrimaryCountryCode") == F.col("PrimaryCountryCode"), "left").drop("PrimaryCountryCode")
else:
    print("⚠️ fips_countries.csv missing!")

print(f"💾 Writing Enriched GKG to: {SILVER_PATH} (Append)")
df.write.format("delta").mode("append").save(SILVER_PATH)

print("✅ GDELT GKG Silver transformation complete.")
spark.stop()
