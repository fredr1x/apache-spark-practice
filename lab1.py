import os
import sys

from pyspark.sql import SparkSession

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

spark = SparkSession.builder \
    .appName("Lab1") \
    .master("local[*]") \
    .getOrCreate()

data = [
    (1, "Yerdaulet", 19),
    (2, "Azamat", 19),
    (3, "Gani", 19)
]

columns = ["id", "name", "age"]

df = spark.createDataFrame(data, columns)

df.show()

spark.stop()
