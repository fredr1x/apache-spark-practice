import os
import sys
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

start = time.perf_counter()
spark = SparkSession.builder \
    .appName("Lab9-LazyEvaluation") \
    .master("local[*]") \
    .getOrCreate()

df = spark.read.csv(
    "people.csv",
    header=True,
    inferSchema=True
)

print("Average age (Spark):")
df.select(avg("age")).show()

print("Age > 25 (Spark):")
df.filter("age > 25").show()

spark.stop()

end = time.perf_counter()
print(f"execution time:{end - start:6f} seconds")
