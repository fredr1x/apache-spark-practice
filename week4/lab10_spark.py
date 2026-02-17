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

print("avg age:")
df.select(avg("age")).show()

print("age > 25:")
df.filter("age > 25").show()

spark.stop()

end = time.perf_counter()
print(f"execution time:{end - start:6f} seconds")

# results
# avg age:
# +--------+
# |avg(age)|
# +--------+
# |   31.25|
# +--------+
#
# age > 25:
# +-------+---+
# |   name|age|
# +-------+---+
# |  Alice| 34|
# |Charlie| 29|
# |   NULL| 40|
# +-------+---+
#
# execution time:16.424013 seconds