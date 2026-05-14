import os
import sys

# PySpark Windows Fixes
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


# ─────────────────────────────────────────────
# UTILITIES
# ─────────────────────────────────────────────

def get_watermark(spark, silver_history_path):
    """
    Returns MAX(ingested_at) from the Silver history Delta table.
    Returns None on cold start (table does not exist yet).
    """
    delta_log = os.path.join(silver_history_path, "_delta_log")
    if os.path.exists(delta_log):
        try:
            res = (
                spark.read.format("delta")
                .load(silver_history_path)
                .select(F.max("ingested_at"))
                .collect()[0][0]
            )
            return str(res) if res else None
        except Exception:
            return None
    return None


def discover_bronze_files(bronze_base, watermark):
    """
    Returns all timestamped .parquet files under bronze_base.
    Explicitly excludes latest.parquet to prevent double-ingestion.
    """
    files = []
    if not os.path.exists(bronze_base):
        return files
        
    watermark_ts = 0
    if watermark:
        try:
            from dateutil import parser
            watermark_ts = parser.parse(watermark).timestamp()
        except Exception:
            watermark_ts = 0

    for fname in os.listdir(bronze_base):
        if fname.endswith(".parquet") and fname != "latest.parquet":
            filepath = os.path.join(bronze_base, fname)
            try:
                if os.path.getmtime(filepath) > watermark_ts:
                    files.append(filepath)
            except OSError:
                continue
    return sorted(files)


def load_bronze_chunked(spark, bronze_files, watermark):
    """
    Reads Bronze parquet files one at a time using Spark and filters by watermark
    BEFORE unioning. Returns a Spark DataFrame of only new rows, or None if empty.
    """
    filtered_dfs = []
    for fpath in bronze_files:
        try:
            df = spark.read.option("mergeSchema", "true").parquet(fpath)
        except Exception as e:
            print(f"[Silver GDELT Events] WARNING: Could not read {fpath}: {e}")
            continue

        if watermark:
            df = df.filter(F.col("ingested_at").cast("string") > watermark)

        if df.limit(1).count() > 0:
            filtered_dfs.append(df)

    if not filtered_dfs:
        return None

    result = filtered_dfs[0]
    for df in filtered_dfs[1:]:
        result = result.unionByName(df, allowMissingColumns=True)
    return result


def load_references(spark, reference_dir):
    """
    Loads all CAMEO and FIPS reference CSVs as broadcast Spark DataFrames.
    """
    ref_event = (
        spark.read.csv(os.path.join(reference_dir, "cameo_eventcodes.csv"), header=True, inferSchema=False)
        .withColumnRenamed("CAMEO", "EventCode")
        .withColumnRenamed("DESCRIPTION", "event_description")
    )
    ref_quad = (
        spark.read.csv(os.path.join(reference_dir, "cameo_quadclass.csv"), header=True, inferSchema=False)
        .withColumnRenamed("code", "QuadClass")
        .withColumnRenamed("label", "quad_description")
    )
    ref_country = (
        spark.read.csv(os.path.join(reference_dir, "fips_countries.csv"), header=True, inferSchema=False)
        .withColumnRenamed("FIPS", "country_code")
        .withColumnRenamed("COUNTRY", "country_name")
    )
    ref_actor_type = (
        spark.read.csv(os.path.join(reference_dir, "cameo_actortypes.csv"), header=True, inferSchema=False)
        .withColumnRenamed("code", "Actor1Type1Code")
        .withColumnRenamed("label", "actor1_type_label")
    )
    return {
        "event": ref_event,
        "quad": ref_quad,
        "country": ref_country,
        "actor_type": ref_actor_type,
    }


def enrich_events(df, refs):
    """
    Pure PySpark enrichment: reference joins + column derivations.
    All reference tables are broadcast-joined (tiny CSV lookups).
    """
    # Zero-pad EventCode to match reference format
    df = df.withColumn("EventCode", F.lpad(F.col("EventCode").cast("string"), 2, "0"))

    # Join event description
    df = df.join(F.broadcast(refs["event"]), on="EventCode", how="left")

    # Derive QuadClass from EventRootCode (CAMEO standard mapping)
    root = F.col("EventRootCode").cast("int")
    df = df.withColumn("QuadClass",
        F.when(root.between(1, 5), F.lit("1"))
         .when(root.between(6, 8), F.lit("2"))
         .when(root.between(9, 14), F.lit("3"))
         .when(root.between(15, 20), F.lit("4"))
         .otherwise(F.lit("0"))
    )

    # Join quad description
    df = df.join(F.broadcast(refs["quad"]), on="QuadClass", how="left")

    # Actor 1 geo country
    ref_actor1_geo = (refs["country"]
        .withColumnRenamed("country_code", "Actor1Geo_CountryCode")
        .withColumnRenamed("country_name", "actor1_country_name"))
    df = df.join(F.broadcast(ref_actor1_geo), on="Actor1Geo_CountryCode", how="left")

    # Actor 2 geo country
    ref_actor2_geo = (refs["country"]
        .withColumnRenamed("country_code", "Actor2Geo_CountryCode")
        .withColumnRenamed("country_name", "actor2_country_name"))
    df = df.join(F.broadcast(ref_actor2_geo), on="Actor2Geo_CountryCode", how="left")

    # Actor 1 type
    bronze_cols = set(df.columns)
    if "Actor1Type1Code" in bronze_cols:
        df = df.withColumn("Actor1Type1Code", F.col("Actor1Type1Code").cast("string"))
        df = df.join(F.broadcast(refs["actor_type"]), on="Actor1Type1Code", how="left")

    # Actor 1 nationality
    if "Actor1CountryCode" in bronze_cols:
        ref_a1nat = (refs["country"]
            .withColumnRenamed("country_code", "Actor1CountryCode")
            .withColumnRenamed("country_name", "actor1_nationality"))
        df = df.join(F.broadcast(ref_a1nat), on="Actor1CountryCode", how="left")

    # Actor 2 nationality
    if "Actor2CountryCode" in bronze_cols:
        ref_a2nat = (refs["country"]
            .withColumnRenamed("country_code", "Actor2CountryCode")
            .withColumnRenamed("country_name", "actor2_nationality"))
        df = df.join(F.broadcast(ref_a2nat), on="Actor2CountryCode", how="left")

    # Build final Silver output — select and rename/cast columns
    cols = df.columns
    final = df.select(
        F.col("GLOBALEVENTID").cast("string").alias("event_id"),
        F.col("SQLDATE").cast("string").alias("event_date"),
        F.col("Actor1Name").alias("actor1_name"),
        F.col("Actor2Name").alias("actor2_name"),
        F.coalesce(F.col("event_description"), F.lit(None).cast("string")).alias("event_description"),
        F.coalesce(F.col("quad_description"), F.lit(None).cast("string")).alias("quad_description"),
        F.coalesce(F.col("actor1_country_name"), F.lit(None).cast("string")).alias("actor1_country_name"),
        F.coalesce(F.col("actor2_country_name"), F.lit(None).cast("string")).alias("actor2_country_name"),
        F.col("GoldsteinScale").cast(DoubleType()).alias("intensity_score"),
        F.col("NumMentions").cast(IntegerType()).alias("mention_count"),
        F.col("AvgTone").cast(DoubleType()).alias("tone_score"),
        (F.col("IsRootEvent").cast(IntegerType()) if "IsRootEvent" in cols else F.lit(None).cast(IntegerType())).alias("is_root_event"),
        (F.col("actor1_type_label") if "actor1_type_label" in cols else F.lit(None).cast("string")).alias("actor1_type"),
        (F.col("actor1_nationality") if "actor1_nationality" in cols else F.lit(None).cast("string")).alias("actor1_nationality"),
        (F.col("actor2_nationality") if "actor2_nationality" in cols else F.lit(None).cast("string")).alias("actor2_nationality"),
        (F.col("DATEADDED").cast("string") if "DATEADDED" in cols else F.lit(None).cast("string")).alias("date_added"),
        F.col("SOURCEURL").alias("source_url"),
        F.col("ingested_at").cast("string").alias("ingested_at"),
    )
    return final


def is_batch_already_written(spark, silver_history_path, batch_min_ingested_at):
    """
    Idempotency guard: returns True if this batch (by MIN ingested_at) was
    already appended to the history table. Prevents duplicate writes on
    accidental re-runs.
    """
    delta_log = os.path.join(silver_history_path, "_delta_log")
    if not os.path.exists(delta_log):
        return False
    count = (
        spark.read.format("delta")
        .load(silver_history_path)
        .filter(F.col("ingested_at") >= str(batch_min_ingested_at))
        .limit(1)
        .count()
    )
    return count > 0


def write_history(df_spark, silver_history_path):
    """
    Append-only write to the history Delta table.
    Preserves full audit trail of every ingestion batch.
    """
    df_spark.write.format("delta").option("mergeSchema", "true").mode("append").save(silver_history_path)


def merge_current(spark, df_spark, silver_current_path):
    """
    MERGE upsert into the _current Delta table on event_id.
    If the event already exists: update all fields (odds may have shifted).
    If the event is new: insert it.

    This means _current always reflects the latest known state of every
    event seen so far, not just the latest batch. This is the correct
    pattern for a dimension table used by the Gold scoring layer.
    """
    os.makedirs(silver_current_path, exist_ok=True)
    delta_log = os.path.join(silver_current_path, "_delta_log")

    if not os.path.exists(delta_log):
        # Cold start: no existing table, just write
        df_spark.write.format("delta").option("mergeSchema", "true").mode("overwrite").save(silver_current_path)
        print("[Silver GDELT Events] _current table created (cold start).")
        return

    target = DeltaTable.forPath(spark, silver_current_path)
    (
        target.alias("current")
        .merge(
            df_spark.alias("incoming"),
            "current.event_id = incoming.event_id"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    print("[Silver GDELT Events] _current MERGE upsert complete.")


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def run(spark):
    """Runs the transform using a caller-managed SparkSession (no spark.stop)."""
    bronze_base    = os.path.join(ROOT, "data", "bronze", "gdelt", "gdelt_events")
    silver_history = os.path.join(ROOT, "data", "silver", "gdelt_events_history")
    silver_current = os.path.join(ROOT, "data", "silver", "gdelt_events_current")
    reference_dir  = os.path.join(ROOT, "reference")

    # Step 1: Watermark
    watermark = get_watermark(spark, silver_history)
    print(f"[Silver GDELT Events] Watermark: {watermark}")

    # Step 2: Discover Bronze files
    bronze_files = discover_bronze_files(bronze_base, watermark)
    if not bronze_files:
        print("[Silver GDELT Events] No Bronze files found. Exiting.")
        return
    print(f"[Silver GDELT Events] Found {len(bronze_files)} Bronze files.")

    # Step 3: Chunked load via Spark — watermark filter applied per file
    df_bronze = load_bronze_chunked(spark, bronze_files, watermark)
    if df_bronze is None:
        print("[Silver GDELT Events] No new data beyond watermark. Exiting.")
        return
    row_count = df_bronze.count()
    print(f"[Silver GDELT Events] Loaded {row_count} new rows after watermark filter.")

    # Step 4: Reference joins via broadcast (pure PySpark)
    refs = load_references(spark, reference_dir)
    df_enriched = enrich_events(df_bronze, refs)
    print(f"[Silver GDELT Events] Enrichment complete.")

    # Step 5: Idempotency guard
    batch_min = df_enriched.select(F.min("ingested_at")).collect()[0][0]
    if is_batch_already_written(spark, silver_history, batch_min):
        print("[Silver GDELT Events] Batch already written. Skipping.")
        return

    # Step 6: Write history (append-only)
    write_history(df_enriched, silver_history)
    print(f"[Silver GDELT Events] History append complete.")

    # Step 7: Merge current (upsert on event_id)
    merge_current(spark, df_enriched, silver_current)

    print(f"[Silver GDELT Events] SUCCESS. {row_count} rows written.")


def main():
    print("[Silver GDELT Events] Initializing Stable Decoder v5...")
    ivy_dir = os.environ.get("IVY_PACKAGE_DIR", "")
    builder = (
        SparkSession.builder
        .appName("PredictIQ_Silver_GDELT_Events")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.parquet.enableVectorizedReader", "false")
    )
    if ivy_dir:
        builder = builder.config("spark.jars.ivy", ivy_dir)
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    try:
        run(spark)
    except Exception as e:
        print(f"[Silver GDELT Events] FAILURE: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()