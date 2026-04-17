import os
import sys
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

def main():
    builder = SparkSession.builder \
        .appName("Check_Gold_News_Sources") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.parquet.enableVectorizedReader", "false")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    
    ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    gold_path = os.path.join(ROOT, "data", "gold", "news_summaries")
    
    if os.path.exists(gold_path):
        df = spark.read.format("delta").load(gold_path)
        print("Sources in gold news summaries:")
        df.select("source", "vol_15m", "vol_24h", "vol_90d").show()
    else:
        print("Gold news summaries table not found.")

    spark.stop()

if __name__ == "__main__":
    main()
