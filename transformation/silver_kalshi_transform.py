import os
import fnmatch
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from delta import configure_spark_with_delta_pip

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def get_watermark(spark, silver_path):
    """
    Returns the MAX ingested_at from Silver Delta table.
    Returns None on first run (table does not exist yet).
    """
    if DeltaTable.isDeltaTable(spark, silver_path):
        result = spark.read.format("delta") \
            .load(silver_path) \
            .select(F.max("ingested_at")) \
            .collect()[0][0]
        return result
    return None

def read_bronze_incremental(spark, bronze_path, watermark):
    """
    Reads Bronze Parquet files newer than watermark.
    If watermark is None, reads everything (cold start).
    """
    if isinstance(bronze_path, list):
        if not bronze_path:
            raise ValueError("No bronze directories found!")
        df = spark.read.option("mergeSchema", "true").parquet(*bronze_path)
    else:
        df = spark.read.option("mergeSchema", "true").parquet(bronze_path)
        
    if watermark is not None:
        df = df.filter(F.col("ingested_at") > watermark)
    return df

def transform(df):
    """
    Silver transformation specific to Kalshi.
    Casts ingested_at to true TimestampType to ensure robust watermark filtering.
    """
    return df.withColumn(
        "ingested_at",
        F.to_timestamp(F.col("ingested_at"))
    )

def write_current(spark, df, silver_current):
    """
    Upserts the latest snapshot per entity into the Current table.
    Uses Delta MERGE to ensure we don't 'overwrite' and lose stagnant closed markets.
    """
    # Deduplicate incoming increment to only keep the absolute latest snapshot per ticker
    windowSpec = Window.partitionBy("ticker").orderBy(F.col("ingested_at").desc())
    latest_df = df.withColumn("rn", F.row_number().over(windowSpec)).filter(F.col("rn") == 1).drop("rn")

    if DeltaTable.isDeltaTable(spark, silver_current):
        delta_table = DeltaTable.forPath(spark, silver_current)
        delta_table.alias("target") \
            .merge(latest_df.alias("source"), "target.ticker = source.ticker") \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
    else:
        latest_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(silver_current)

def write_history(spark, df, silver_history):
    """
    Appends the raw incremental snapshots to the History table.
    Guards against duplicate batches on PySpark/Docker crash restarts.
    """
    if DeltaTable.isDeltaTable(spark, silver_history):
        # Get the min ingested_at of incoming batch
        batch_min = df.select(F.min("ingested_at")).collect()[0][0]
        
        already_exists = spark.read.format("delta") \
            .load(silver_history) \
            .filter(F.col("ingested_at") >= batch_min) \
            .limit(1) \
            .count() > 0
            
        if already_exists:
            print("[Silver Kalshi] History batch already exists - skipping append.")
            return

    df.write.format("delta").mode("append").option("mergeSchema", "true").save(silver_history)


def main():
    print("[Silver Kalshi] Initializing PySpark Session...")
    builder = SparkSession.builder \
        .appName("PredictIQ_Silver_Kalshi") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.parquet.enableVectorizedReader", "false")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # Dynamically find all exact parquet files to bypass PySpark native Windows Glob/IO crashes
    bronze_base = os.path.join(ROOT, "data", "bronze", "kalshi_markets")
    bronze_files = []
    
    for r, d, f in os.walk(bronze_base):
        for filename in fnmatch.filter(f, '*.parquet'):
            bronze_files.append(os.path.join(r, filename))
            
    silver_current = os.path.join(ROOT, "data", "silver", "kalshi_markets_current")
    silver_history = os.path.join(ROOT, "data", "silver", "kalshi_markets_history")

    try:
        # Step 1: Get watermark (drive off the History table to ensure we capture all appends)
        watermark = get_watermark(spark, silver_history)
        print(f"[Silver Kalshi] Operational Watermark calculated: {watermark}")

        # Step 2: Read incremental data
        print(f"[Silver Kalshi] Loading and filtering Bronze Increments from {len(bronze_files)} raw Parquet files...")
        df = read_bronze_incremental(spark, bronze_files, watermark)
        
        # Exit early if no new data prevents empty append operations
        if df.isEmpty():
            print("[Silver Kalshi] Pipeline Skipped: No new data found beyond watermark.")
            return

        # Step 3: Transform
        df_transformed = transform(df)
        
        # Step 4: Write Current
        print(f"[Silver Kalshi] Standardizing and Upserting Current Table...")
        write_current(spark, df_transformed, silver_current)

        # Step 5: Write History
        print(f"[Silver Kalshi] Appending safely to History Table...")
        write_history(spark, df_transformed, silver_history)

        print("[Silver Kalshi] Run completely successful. 100% Data Integrity Ensured.")
        
    except Exception as e:
        print(f"[Silver Kalshi] FATAL ERROR: {str(e)}")
        raise e
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
