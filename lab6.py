import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

spark = SparkSession.builder \
    .appName("Lab6") \
    .master("local[*]") \
    .getOrCreate()

df = spark.read.csv(
    "people.csv",
    header=True,
    inferSchema=True
)

print("dataframe:")
df.show()

print("schema:")
df.printSchema()

print("rows with null values:")
df.filter(
    col("name").isNull() | col("age").isNull()
).show()

spark.stop()
