import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta import configure_spark_with_delta_pip

def main():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    
    builder = SparkSession.builder \
        .appName("Check_Silver_Range") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.parquet.enableVectorizedReader", "false")
    
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    
    ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    gkg_path = os.path.join(ROOT, "data", "silver", "gdelt_gkg_history")
    
    if os.path.exists(gkg_path):
        df = spark.read.format("delta").load(gkg_path)
        df.select(F.min("ingested_at"), F.max("ingested_at")).show()
        
        # Check if ts casting actually results in nulls
        df_ts = df.withColumn("ts", F.col("ingested_at").cast("timestamp").cast("long"))
        df_ts.select("ingested_at", "ts").show(5, False)
        print(f"Nulls in ts: {df_ts.filter('ts IS NULL').count()}")
    else:
        print("GKG Path missing")

    spark.stop()

if __name__ == "__main__":
    main()
