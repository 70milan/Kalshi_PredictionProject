import os
import sys
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

def main():
    builder = SparkSession.builder \
        .appName("Check_News_Sources_File") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.parquet.enableVectorizedReader", "false")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    
    ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    silver_path = os.path.join(ROOT, "data", "silver", "news_articles_enriched")
    
    output_path = os.path.join(ROOT, "scratch", "news_source_check.txt")
    with open(output_path, "w") as f:
        if os.path.exists(silver_path):
            df = spark.read.format("delta").load(silver_path)
            f.write("Sources in silver news (count):\n")
            counts = df.groupBy("source").count().collect()
            for row in counts:
                f.write(f"{row}\n")
        else:
            f.write("Silver news table not found.\n")

        f.write("\nBronze source inspection:\n")
        news_sources = ["bbc", "cnn", "foxnews", "nypost", "nyt", "thehindu"]
        for source in news_sources:
            p = os.path.join(ROOT, "data", "bronze", source)
            if os.path.exists(p):
                files = [f for f in os.listdir(p) if f.endswith(".parquet") and f != "latest.parquet"]
                if files:
                    try:
                        sample_file = os.path.join(p, files[0])
                        sdf = spark.read.parquet(sample_file)
                        f.write(f"\nSource: {source}\n")
                        f.write(f"Columns: {sdf.columns}\n")
                        if "source" in sdf.columns:
                            sample_source = sdf.select("source").limit(1).collect()
                            f.write(f"Sample source value: {sample_source}\n")
                        else:
                            f.write("WARNING: 'source' column is missing!\n")
                    except Exception as e:
                        f.write(f"Error reading {source}: {str(e)}\n")
            else:
                f.write(f"Bronze path missing for {source}\n")

    spark.stop()

if __name__ == "__main__":
    main()
