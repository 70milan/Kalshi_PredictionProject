import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta import configure_spark_with_delta_pip

def main():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    
    builder = SparkSession.builder \
        .appName("Check_Silver_Range_File") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.parquet.enableVectorizedReader", "false")
    
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    
    ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    gkg_path = os.path.join(ROOT, "data", "silver", "gdelt_gkg_history")
    
    with open(os.path.join(ROOT, "scratch", "silver_gkg_check.txt"), "w") as f:
        if os.path.exists(gkg_path):
            df = spark.read.format("delta").load(gkg_path)
            stats = df.select(F.min("ingested_at"), F.max("ingested_at")).collect()[0]
            f.write(f"Silver Range: {stats[0]} to {stats[1]}\n")
            
            # Check if ts casting actually results in nulls
            df_ts = df.withColumn("ts", F.col("ingested_at").cast("timestamp").cast("long"))
            f.write("Samples:\n")
            samples = df_ts.select("ingested_at", "ts").limit(5).collect()
            for s in samples:
                f.write(f"{s}\n")
            
            null_count = df_ts.filter('ts IS NULL').count()
            f.write(f"Nulls in ts: {null_count}\n")
        else:
            f.write("GKG Path missing\n")

    spark.stop()

if __name__ == "__main__":
    main()
