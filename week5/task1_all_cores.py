import time
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

spark = (
    SparkSession.builder
    .appName("multi-core")
    .master("local[*]")
    .getOrCreate()
)

df = spark.range(0, 50_000_000_000)

start = time.time()

result = df.select((col("id") * 2 + 1).alias("value")).count()

end = time.time()
print(f"Multi-core duration: {end - start:.2f} seconds")

spark.stop()
# Multi-core duration: 3.93 seconds