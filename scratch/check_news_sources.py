import os
import sys
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

def main():
    builder = SparkSession.builder \
        .appName("Check_Silver_News_Sources") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.parquet.enableVectorizedReader", "false")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    
    ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    silver_path = os.path.join(ROOT, "data", "silver", "news_articles_enriched")
    
    if not os.path.exists(silver_path):
        print(f"Silver path does not exist: {silver_path}")
        return

    df = spark.read.format("delta").load(silver_path)
    
    print("Sources in silver news:")
    df.groupBy("source").count().show()
    
    print("\nSample sources from bronze (first 1 row each):")
    news_sources = ["bbc", "cnn", "foxnews", "nypost", "nyt", "thehindu"]
    for source in news_sources:
        p = os.path.join(ROOT, "data", "bronze", source)
        if os.path.exists(p):
            files = [f for f in os.listdir(p) if f.endswith(".parquet") and f != "latest.parquet"]
            if files:
                sample_file = os.path.join(p, files[0])
                sdf = spark.read.parquet(sample_file)
                print(f"Source {source} columns: {sdf.columns}")
                if "source" in sdf.columns:
                   sdf.select("source").show(1)
                else:
                   print(f"WARNING: source column missing in {source}")

    spark.stop()

if __name__ == "__main__":
    main()
