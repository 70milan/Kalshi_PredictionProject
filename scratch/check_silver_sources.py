import os
import sys
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

def main():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    
    builder = SparkSession.builder \
        .appName("Check_Silver_Sources") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.parquet.enableVectorizedReader", "false")
    
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    
    # Check what is actually in silver
    silver_path = os.path.join(os.getcwd(), "data", "silver", "news_articles_enriched")
    if os.path.exists(silver_path):
        df = spark.read.format("delta").load(silver_path)
        print("Unique Sources in Silver:")
        df.select("source").distinct().show()
        
        print("Sample dates for non-BBC/CNN sources:")
        df.filter("source NOT IN ('BBC', 'CNN')").select("source", "published_at").show(10)
    else:
        print("Silver path missing!")

    spark.stop()

if __name__ == "__main__":
    main()
