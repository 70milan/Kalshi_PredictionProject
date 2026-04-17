import os
import sys
import pandas as pd
from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.getOrCreate()
    ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
    p = os.path.join(ROOT, "data", "bronze", "foxnews")
    files = [f for f in os.listdir(p) if f.endswith(".parquet") and f != "latest.parquet"]
    if files:
        df = spark.read.parquet(os.path.join(p, files[0]))
        pdf = df.toPandas()
        print("Fox News Bronze (first 5 published_at):")
        print(pdf["published_at"].head(5))
        
        parsed = pd.to_datetime(pdf["published_at"], errors="coerce", utc=True)
        print("\nParsed with pd.to_datetime:")
        print(parsed.head(5))
        
        null_count = parsed.isna().sum()
        print(f"\nNull count after parsing: {null_count} out of {len(pdf)}")

    spark.stop()

if __name__ == "__main__":
    main()
