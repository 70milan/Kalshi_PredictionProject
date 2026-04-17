import os
import sys
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

def main():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    
    builder = SparkSession.builder \
        .appName("Check_News_History_Count") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.parquet.enableVectorizedReader", "false")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    
    ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    history_path = os.path.join(ROOT, "data", "gold", "news_summaries_history")
    
    if os.path.exists(history_path):
        df = spark.read.format("delta").load(history_path)
        print(f"Total rows in gold.news_summaries_history: {df.count()}")
    else:
        print("News history path missing.")

    spark.stop()

if __name__ == "__main__":
    main()
