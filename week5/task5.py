from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("task3") \
    .master("local[*]") \
    .getOrCreate()

spark.stop()
