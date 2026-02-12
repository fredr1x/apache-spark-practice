import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

spark = SparkSession.builder \
    .appName("Lab5") \
    .master("local[*]") \
    .getOrCreate()

data = [
    (1, "Yerdaulet", 19, 3.4, "Kazakhstan"),
    (2, "Azamat", 24, 3.3, "Spain"),
    (3, "Gani", 29, 3.5, "USA"),
    (4, "Ansat", 31, 3.6, "USA"),
    (5, "Bakdaulet", 21, 3.7, "Spain"),
]

columns = ["id", "name", "age", "GPA", "country"]

df = spark.createDataFrame(data, columns)

df.show()

print("groupBy country:")
df.groupBy("country").count().show()

print("average GPA:")
df.select(avg("GPA").alias("avg_GPA")).show()

spark.stop()
