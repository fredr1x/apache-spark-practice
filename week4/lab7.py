import os
import sys

from pyspark.sql import SparkSession

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

spark = SparkSession.builder \
                    .appName("Lab7") \
                    .master("local[*]") \
                    .getOrCreate()

df = spark.read.csv("people.csv", header=True, inferSchema=True)

df.show()

cleaned_df = df.dropna()
print("Cleaned dataframe:")
cleaned_df.show()

spark.stop()
