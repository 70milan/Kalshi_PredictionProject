import os
import sys
from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.getOrCreate()
    
    ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    fox_p = os.path.join(ROOT, "data", "bronze", "foxnews")
    files = [f for f in os.listdir(fox_p) if f.endswith(".parquet") and f != "latest.parquet"]
    
    if files:
        df = spark.read.parquet(os.path.join(fox_p, files[0]))
        print("Fox News Bronze Schema:")
        df.printSchema()
        print("Fox News Bronze Sample (published_at/pubDate/published):")
        df.select([c for c in df.columns if "pub" in c.lower() or "date" in c.lower()]).show(5)

    spark.stop()

if __name__ == "__main__":
    main()
