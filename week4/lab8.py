import os
import sys

from pyspark.sql import SparkSession

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

spark = SparkSession.builder \
                    .appName("Lab8") \
                    .master("local[*]") \
                    .getOrCreate()

df = spark.read.csv("people.csv", header=True, inferSchema=True)

df.show()
df.createOrReplaceTempView("people")

print("people with age > 25:")
spark.sql("select name, age "
          "from people "
          "where age > 25 "
).show()

print("average age:")
spark.sql("select avg(age) as avg_age "
          "from people"
).show()

spark.stop()
