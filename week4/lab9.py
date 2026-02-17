import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

spark = SparkSession.builder \
    .appName("Lab9") \
    .master("local[*]") \
    .getOrCreate()

df = spark.read.csv(
    "people.csv",
    header=True,
    inferSchema=True
)

filtered_df = df.filter(col("age") > 25)
filtered_df.show()

spark.stop()
