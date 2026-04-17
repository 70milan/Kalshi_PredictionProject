import os
import sys
from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.getOrCreate()
    ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
    p = os.path.join(ROOT, "data", "bronze", "bbc")
    files = [f for f in os.listdir(p) if f.endswith(".parquet") and f != "latest.parquet"]
    if files:
        df = spark.read.parquet(os.path.join(p, files[0]))
        print(f"BBC Columns: {df.columns}")
        df.select("published_at").show(1)

    spark.stop()

if __name__ == "__main__":
    main()
