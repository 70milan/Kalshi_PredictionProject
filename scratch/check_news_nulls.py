import os
import sys
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

def main():
    builder = SparkSession.builder \
        .appName("Check_Silver_News_Nulls") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.parquet.enableVectorizedReader", "false")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    
    ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    silver_path = os.path.join(ROOT, "data", "silver", "news_articles_enriched")
    
    df = spark.read.format("delta").load(silver_path)
    
    print("Source counts where published_at is NOT NULL:")
    df.filter("published_at IS NOT NULL").groupBy("source").count().show()
    
    print("Source counts where published_at IS NULL:")
    df.filter("published_at IS NULL").groupBy("source").count().show()

    spark.stop()

if __name__ == "__main__":
    main()
