from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.master("local[*]").appName("DataSkew").getOrCreate()

skewed = spark.range(0, 9_000_000_000).withColumn("key", F.lit("heavy"))

small = spark.range(0, 100).withColumn("key", F.concat(F.lit("k_"), F.col("id")))

df = skewed.unionByName(small)

df.groupBy("key").count().show()
