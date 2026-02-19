import os
import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["PYSPARK_PYTHON"] = sys.executable

start = time.perf_counter()
spark = SparkSession.builder.appName("task1").master("local[*]").getOrCreate()

data = [(10,), (20,), (30,), (40,), (50,)]
column = ["numbers"]

# task 1
print("task 1")
df = spark.createDataFrame(data, column)
df.show()

# task 2
print("task 2")
df2 = df.withColumn("double_value", col("numbers") * 2)
df2.show()

# task 3
print("task 3")
filtered_df2 = df2.filter(col("numbers") > 25)
filtered_df2.show()

# task 4
print("task 4")
json_df = spark.read.option("multiLine", "true").json("people.json")
json_df.printSchema()
json_df.show()

# task 5
print("task 5")
age_gt_21 = json_df.filter(col("age") > 21)
age_gt_21.show()

# task 6
print("task 6")
adults_json = json_df.withColumn("is_adult", when(col("age") > 18, "YES").otherwise("NO"))
adults_json.show()

# task 7
print("task 7")
city_df = json_df.groupBy("city").count()
city_df.show()

# task 8
print("task 8")
desc_order_age = json_df.orderBy(desc("age"))
desc_order_age.show()

# task 9
print("task 9")
select_scores_as_cols = json_df.select("name", col("scores.math").alias("math_score"), col("scores.english").alias("english_score"))
select_scores_as_cols.show()

print("task 10")

data_for_rdd = ['spark','hadoop','spark','python','spark']

rdd = spark.sparkContext.parallelize(data_for_rdd)

word_count = rdd.map(lambda word: (word, 1)) \
                .reduceByKey(lambda a, b: a + b)

print(word_count.collect())

# Task 11
print("task 11")
word_count_df = word_count.toDF(["word", "count"])
word_count_df.show()

# Task 12
# did not work needs hadoop
# print("task 12")
# city_group = json_df.groupBy("city").count()
#
# city_group.write \
#     .mode("overwrite") \
#     .option("header", "true") \
#     .csv("city_counts")

# task 13
print("task 13")
avg_age = json_df.select(avg("age"))
avg_age.show()

# task 14
print("task 14")
oldest = json_df.select(max("age"))

end = time.perf_counter()
print(end - start)
# Final reflection
# actions: show(), count(), write(), printSchema()
# transformations: select(), withColumn(), orderBy()
