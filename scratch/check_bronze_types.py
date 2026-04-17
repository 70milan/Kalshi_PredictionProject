import os
import sys
from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.getOrCreate()
    ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
    sources = ["bbc", "foxnews", "nypost", "nyt", "thehindu"]
    for source in sources:
        p = os.path.join(ROOT, "data", "bronze", source)
        if os.path.exists(p):
            files = [f for f in os.listdir(p) if f.endswith(".parquet") and f != "latest.parquet"]
            if files:
                df = spark.read.parquet(os.path.join(p, files[0]))
                dtype = [typ for name, typ in df.dtypes if name == "published_at"]
                print(f"{source} published_at type: {dtype}")

    spark.stop()

if __name__ == "__main__":
    main()
