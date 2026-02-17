from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("task3") \
    .master("local[*]") \
    .getOrCreate()

df = spark.read.json("task3.json")

df.cache()     
df.count()

valid_df = df.filter(col("_corrupt_record").isNull())
valid_df.show()
