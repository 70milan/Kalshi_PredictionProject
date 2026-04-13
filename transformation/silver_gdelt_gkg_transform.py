import os
import sys
import fnmatch

# PySpark Windows Fixes
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

# Windows compatibility: ensure HADOOP_HOME bin is on PATH if env var is set
_hadoop_home = os.environ.get("HADOOP_HOME", "")
if _hadoop_home:
    os.environ["PATH"] = os.path.join(_hadoop_home, "bin") + ";" + os.environ.get("PATH", "")

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
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
    Self-healing: if the table is wiped, returns None and processes all Bronze.
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


def discover_bronze_files(bronze_base):
    """
    Recursively walks bronze_base and returns all timestamped .parquet files.
    Explicitly excludes latest.parquet to prevent double-ingestion.
    Uses os.walk + fnmatch for Windows compatibility (PySpark glob is broken on Windows).
    """
    files = []
    if not os.path.exists(bronze_base):
        return files
    for root, dirs, fnames in os.walk(bronze_base):
        for fname in fnmatch.filter(fnames, "*.parquet"):
            if fname != "latest.parquet":
                files.append(os.path.join(root, fname))
    return sorted(files)


def load_bronze_chunked(spark, bronze_files, watermark):
    """
    Reads Bronze parquet files one at a time using Spark and filters by watermark
    BEFORE unioning. This prevents loading the full Bronze GKG history into
    driver memory — GKG files are wider and heavier than Events files.

    Returns a Spark DataFrame of only new rows, or None if empty.
    Each file is read individually and filtered; only matching rows are kept
    before the union. This is O(new rows) in memory, not O(all Bronze).
    """
    filtered_dfs = []

    for fpath in bronze_files:
        try:
            df = spark.read.option("mergeSchema", "true").parquet(fpath)
        except Exception as e:
            print(f"[Silver GDELT GKG] WARNING: Could not read {fpath}: {e}")
            continue

        if watermark:
            df = df.filter(F.col("ingested_at").cast("string") > watermark)

        # Cheap check: skip files with zero rows after filter before adding to union
        # .limit(1).count() is O(1) per partition — avoids full scan just to count
        if df.limit(1).count() > 0:
            filtered_dfs.append(df)

    if not filtered_dfs:
        return None

    # Union all filtered slices — only new rows are in memory at this point
    result = filtered_dfs[0]
    for df in filtered_dfs[1:]:
        result = result.unionByName(df, allowMissingColumns=True)

    return result


def clean_gkg_list(column_name):
    """
    Cleans GDELT GKG semicolon-separated lists.
    Strips the comma-offset suffix from each element.
    Example: 'Donald Trump,15;Joe Biden,42' -> ['Donald Trump', 'Joe Biden']

    Pattern:
      1. Split by semicolon into array
      2. For each element, strip everything after the comma (the offset)
      3. Filter out empty strings
    """
    return F.expr(
        f"filter("
        f"  transform(split({column_name}, ';'), x -> regexp_replace(x, ',.*', '')),"
        f"  x -> x != ''"
        f")"
    )


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
        .filter(F.col("ingested_at").cast("string") >= str(batch_min_ingested_at))
        .limit(1)
        .count()
    )
    return count > 0


def write_history(df_silver, silver_history_path):
    """
    Append-only write to the history Delta table.
    Preserves full audit trail of every ingestion batch.
    """
    df_silver.write.format("delta").option("mergeSchema", "true").mode("append").save(silver_history_path)


def merge_current(spark, df_silver, silver_current_path):
    """
    MERGE upsert into the _current Delta table on gkg_record_id.
    If the record already exists: update all fields.
    If the record is new: insert it.

    GKG records are document-level snapshots so updates are rare in practice,
    but the MERGE pattern keeps _current consistent with the full history
    and prevents stale rows if a record is re-processed.
    """
    os.makedirs(silver_current_path, exist_ok=True)
    delta_log = os.path.join(silver_current_path, "_delta_log")

    if not os.path.exists(delta_log):
        # Cold start: no existing table, just write
        df_silver.write.format("delta").option("mergeSchema", "true").mode("overwrite").save(silver_current_path)
        print("[Silver GDELT GKG] _current table created (cold start).")
        return

    target = DeltaTable.forPath(spark, silver_current_path)
    (
        target.alias("current")
        .merge(
            df_silver.alias("incoming"),
            "current.gkg_record_id = incoming.gkg_record_id"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    print("[Silver GDELT GKG] _current MERGE upsert complete.")


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def main():
    print("[Silver GDELT GKG] Initializing GKG Exploder v5...")

    ivy_dir = os.environ.get("IVY_PACKAGE_DIR", "")
    builder = (
        SparkSession.builder
        .appName("PredictIQ_Silver_GDELT_GKG")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.parquet.enableVectorizedReader", "false")
    )
    if ivy_dir:
        builder = builder.config("spark.jars.ivy", ivy_dir)
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    bronze_base    = os.path.join(ROOT, "data", "bronze", "gdelt", "gdelt_gkg")
    silver_history = os.path.join(ROOT, "data", "silver", "gdelt_gkg_history")
    silver_current = os.path.join(ROOT, "data", "silver", "gdelt_gkg_current")

    try:
        # Step 1: Watermark
        watermark = get_watermark(spark, silver_history)
        print(f"[Silver GDELT GKG] Watermark: {watermark}")

        # Step 2: Discover Bronze files
        bronze_files = discover_bronze_files(bronze_base)
        if not bronze_files:
            print("[Silver GDELT GKG] No Bronze files found. Exiting.")
            return
        print(f"[Silver GDELT GKG] Found {len(bronze_files)} Bronze files.")

        # Step 3: Chunked Spark load — watermark filter applied per file
        df_bronze = load_bronze_chunked(spark, bronze_files, watermark)
        if df_bronze is None:
            print("[Silver GDELT GKG] No new data beyond watermark. Exiting.")
            return

        # Step 4: Column detection — V2 fields preferred, fallback to V1
        persons_col = "V2Persons" if "V2Persons" in df_bronze.columns else "Persons"
        themes_col  = "V2Themes"  if "V2Themes"  in df_bronze.columns else "Themes"
        print(f"[Silver GDELT GKG] Using persons_col={persons_col}, themes_col={themes_col}")

        # Step 5: Enrichment — explode GKG lists into clean Spark arrays
        df_enriched = (
            df_bronze
            .withColumn("persons_array", clean_gkg_list(persons_col))
            .withColumn("themes_array",  clean_gkg_list(themes_col))
            .withColumn("orgs_array",    clean_gkg_list("V2Organizations"))
            .withColumn("locs_array",    clean_gkg_list("V2Locations"))
            .withColumn("names_array",
                        F.when(F.col("AllNames").isNotNull(), clean_gkg_list("AllNames"))
                         .otherwise(F.array()))
        )

        # Step 6: Select human-readable Silver schema
        # New fields use coalesce to handle old Bronze files that lack the columns
        bronze_cols = set(df_enriched.columns)
        df_silver = df_enriched.select(
            F.col("GKGRECORDID").alias("gkg_record_id"),
            F.col("DATE").cast("string").alias("event_date"),
            F.col("SOURCEURL").alias("doc_url"),
            F.col("themes_array"),
            F.col("persons_array"),
            F.col("orgs_array"),
            F.col("locs_array"),
            F.col("names_array"),
            F.col("V2Tone").alias("raw_tone_stats"),
            (F.col("V2Counts") if "V2Counts" in bronze_cols
             else F.lit(None).cast("string")).alias("raw_counts"),
            (F.col("Amounts") if "Amounts" in bronze_cols
             else F.lit(None).cast("string")).alias("raw_amounts"),
            (F.col("GCAM") if "GCAM" in bronze_cols
             else F.lit(None).cast("string")).alias("raw_gcam"),
            F.col("ingested_at").cast("string").alias("ingested_at"),
        )

        # Step 7: Idempotency guard
        batch_min = df_silver.select(F.min("ingested_at")).collect()[0][0]
        if is_batch_already_written(spark, silver_history, batch_min):
            print("[Silver GDELT GKG] Batch already written. Skipping.")
            return

        # Step 8: Write history (append-only)
        write_history(df_silver, silver_history)
        print("[Silver GDELT GKG] History append complete.")

        # Step 9: Merge current (upsert on gkg_record_id)
        merge_current(spark, df_silver, silver_current)

        print("[Silver GDELT GKG] SUCCESS.")

    except Exception as e:
        print(f"[Silver GDELT GKG] FAILURE: {e}")
        raise e
    finally:
        spark.stop()


if __name__ == "__main__":
    main()