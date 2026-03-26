import os
os.environ['HDOOP_HOME'] = 'C:\\hadoop'
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Test") \
    .master("local[*]") \
    .getOrCreate()

df = spark.range(5)

df.write.mode("overwrite").csv("data/output/test")

print("✅ Working")