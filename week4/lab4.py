import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

spark = SparkSession.builder \
    .appName("Lab4") \
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

df_modified = df.withColumn("country", lit("Kazakhstan"))

print("Modified dataframe:")
df_modified.show()

print("Original dataframe:")
df.show()

spark.stop()
