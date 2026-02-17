import os
import sys

from pyspark.sql import SparkSession

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

spark = SparkSession.builder \
    .appName("Lab2") \
    .master("local[*]") \
    .getOrCreate()

data = [
    (1, "Yerdaulet", 19),
    (2, "Azamat", 19),
    (3, "Gani", 19)
]

columns = ["id", "name", "age"]

df = spark.createDataFrame(data, columns)

print(f"Total rows: {df.count()}")
print(f"ID column count: {df.select('id').count()}")
print(f"Name column count: {df.select('name').count()}")

spark.stop()
