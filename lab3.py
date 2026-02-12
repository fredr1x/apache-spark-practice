import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

spark = SparkSession.builder \
    .appName("Lab3") \
    .master("local[*]") \
    .getOrCreate()

data = [
    (1, "Yerdaulet", 19),
    (2, "Azamat", 24),
    (3, "Gani", 29),
    (4, "Ansat", 31),
    (5, "Bakdaulet", 21),
]

columns = ["id", "name", "age"]

df = spark.createDataFrame(data, columns)

df.show()

# filter by column
filtered_by_column = df.filter(col("age") > 25)
print("Filtered by column age > 25")
filtered_by_column.show()

# sql style filter
filtered_sql = df.filter("age > 25")
print("Filtered by sql age > 25")
filtered_sql.show()

filtered_by_column.show()

spark.stop()
