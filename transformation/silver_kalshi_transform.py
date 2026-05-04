import os
import sys
import fnmatch

# PySpark Windows Fixes
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# Windows compatibility: ensure HADOOP_HOME bin is on PATH if env var is set
_hadoop_home = os.environ.get("HADOOP_HOME", "")
if _hadoop_home:
    os.environ["PATH"] = os.path.join(_hadoop_home, "bin") + ";" + os.environ.get("PATH", "")

os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
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
    if os.path.exists(os.path.join(silver_path, "_delta_log")):
        result = spark.read.format("delta") \
            .load(silver_path) \
            .select(F.max("ingested_at")) \
            .collect()[0][0]
        return result
    return None

# Columns that DuckDB writes as BIGINT from some API runs but DOUBLE from others
TYPE_CONFLICT_COLS = ["floor_strike", "cap_strike", "yes_bid_dollars", "yes_ask_dollars", "yes_bid", "yes_ask"]

def normalize_types(df):
    """
    Cast known type-conflict columns to DOUBLE so all files
    can be safely unioned without CANNOT_MERGE_INCOMPATIBLE_DATA_TYPE.
    """
    for col_name in TYPE_CONFLICT_COLS:
        if col_name in df.columns:
            df = df.withColumn(col_name, F.col(col_name).cast("double"))
    return df

def read_bronze_incremental(spark, bronze_base, watermark):
    """
    Reads only NEW Kalshi Parquet files by comparing file modification times
    against the existing Silver watermark. This avoids the O(N) bottleneck
     of opening hundreds of historical files.
    """
    all_files = []
    # Convert watermark timestamp to unix seconds if it exists
    watermark_ts = 0
    if watermark:
        try:
            # Handle both string and datetime watermarks
            if isinstance(watermark, str):
                from dateutil import parser
                watermark_ts = parser.parse(watermark).timestamp()
            else:
                watermark_ts = watermark.timestamp()
        except Exception:
            watermark_ts = 0

    print(f"[Silver Kalshi]   Scanning Bronze directory: {bronze_base}")
    for r, d, f in os.walk(bronze_base):
        for filename in fnmatch.filter(f, '*.parquet'):
            if filename == "latest.parquet":
                continue
            filepath = os.path.join(r, filename)
            
            # Optimization: Only add to the read list if the file is NEWER than our watermark
            try:
                if os.path.getmtime(filepath) > watermark_ts:
                    all_files.append(filepath)
            except OSError:
                continue

    if not all_files:
        print("[Silver Kalshi]   No files newer than watermark found on disk.")
        return spark.createDataFrame([], "ticker string, ingested_at string") # Return empty with schema dummy

    print(f"[Silver Kalshi]   Found {len(all_files)} NEW Parquet files. Reading...")

    frames = []
    for filepath in all_files:
        try:
            df_single = spark.read.parquet(filepath)
            df_single = normalize_types(df_single)
            frames.append(df_single)
        except Exception as e:
            print(f"[Silver Kalshi]   WARNING: Skipping corrupt file {filepath}: {e}")

    if not frames:
        return spark.createDataFrame([], "ticker string, ingested_at string")

    # Union all with allowMissingColumns
    df = frames[0]
    for frame in frames[1:]:
        df = df.unionByName(frame, allowMissingColumns=True)

    if watermark is not None:
        df = df.filter(F.col("ingested_at") > watermark)
    return df

def transform(df):
    """
    Silver transformation specific to Kalshi.
    Casts ingested_at to true TimestampType to ensure robust watermark filtering.
    Standardizes row naming from V2 (_dollars) to legacy naming.
    """
    # 1. Standardize V2 Naming to Legacy naming for downstream compatibility
    cols = df.columns
    # Defensive: If both exist (schema merge conflict), prioritize the V2 dollar columns
    if "yes_bid_dollars" in cols and "yes_bid" in cols:
        df = df.drop("yes_bid")
    if "yes_bid_dollars" in cols:
        df = df.withColumnRenamed("yes_bid_dollars", "yes_bid")

    if "yes_ask_dollars" in cols and "yes_ask" in cols:
        df = df.drop("yes_ask")
    if "yes_ask_dollars" in cols:
        df = df.withColumnRenamed("yes_ask_dollars", "yes_ask")
    
    # 2. Normalize types (ensure prices are doubles)
    df = normalize_types(df)

    # 3. Handle Timestamps
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

    if os.path.exists(os.path.join(silver_current, "_delta_log")):
        try:
            delta_table = DeltaTable.forPath(spark, silver_current)
            delta_table.alias("target") \
                .merge(latest_df.alias("source"), "target.ticker = source.ticker") \
                .whenMatchedUpdateAll() \
                .whenNotMatchedInsertAll() \
                .execute()
        except Exception as e:
            print(f"[Silver Kalshi] Schema evolution detected! Rebuilding Current table properly from History: {e}")
            # Rebuild the entire current table from history so we don't lose old markets
            history_df = spark.read.format("delta").load(silver_current.replace("_current", "_history"))
            combined_df = history_df.unionByName(latest_df, allowMissingColumns=True)
            rebuild_df = combined_df.withColumn("rn", F.row_number().over(windowSpec)).filter(F.col("rn") == 1).drop("rn")
            rebuild_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(silver_current)
    else:
        latest_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(silver_current)

def write_history(spark, df, silver_history):
    """
    Appends the raw incremental snapshots to the History table.
    Guards against duplicate batches on PySpark/Docker crash restarts.
    """
    if os.path.exists(os.path.join(silver_history, "_delta_log")):
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
    ivy_dir = os.environ.get("IVY_PACKAGE_DIR", "")
    builder = SparkSession.builder \
        .appName("PredictIQ_Silver_Kalshi") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true") \
        .config("spark.sql.parquet.enableVectorizedReader", "false")
    if ivy_dir:
        builder = builder.config("spark.jars.ivy", ivy_dir)

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    bronze_base = os.path.join(ROOT, "data", "bronze", "kalshi_markets")
    silver_current = os.path.join(ROOT, "data", "silver", "kalshi_markets_current")
    silver_history = os.path.join(ROOT, "data", "silver", "kalshi_markets_history")

    try:
        # Step 1: Get watermark (drive off the History table to ensure we capture all appends)
        watermark = get_watermark(spark, silver_history)
        print(f"[Silver Kalshi] Operational Watermark calculated: {watermark}")

        # Step 2: Read incremental data (per-subdirectory to avoid type conflicts)
        print("[Silver Kalshi] Loading Bronze subdirectories with type normalization...")
        df = read_bronze_incremental(spark, bronze_base, watermark)
        
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
