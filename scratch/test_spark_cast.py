import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
df = spark.createDataFrame([("2026-04-01T00:18:11",)], ["ts_str"])
df.withColumn("ts_cast", F.col("ts_str").cast("timestamp")).show()

spark.stop()
