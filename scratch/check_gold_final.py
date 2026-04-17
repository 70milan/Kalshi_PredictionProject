import os
import sys
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

def main():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    
    builder = SparkSession.builder \
        .appName("Check_Gold_Final") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.parquet.enableVectorizedReader", "false")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    
    ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    gold_path = os.path.join(ROOT, "data", "gold", "gdelt_summaries")
    
    if os.path.exists(gold_path):
        df = spark.read.format("delta").load(gold_path)
        print("Count > 0 in active GDELT gold:")
        df.filter("vol_spike_multiplier > 0").select("entity_name", "vol_15m", "vol_spike_multiplier").show(10)
        print(f"Total rows in active gold: {df.count()}")
        print(f"Rows with multiplier > 0: {df.filter('vol_spike_multiplier > 0').count()}")
    else:
        print("Gold path missing.")

    spark.stop()

if __name__ == "__main__":
    main()
