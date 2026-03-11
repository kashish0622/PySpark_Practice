from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.appName("Mathematical_functions")\
    .master("local[*]")\
    .config("spark.driver.memory", "5g")\
    .config("spark.executor.memory", "3g")\
    .config("spark.executor.cores", "3")\
    .getOrCreate()
print("SparkSession created for Mathematical_functions")

data = [
    (101, "Ayush", -25.5, 4),
    (102, "Riya", -10.2, 9),
    (103, "Karan", 15.7, 16),
    (104, "Sneha", 8.3, 25),
    (105, "Rahul", -3.6, 36)
]

columns = ["id", "name", "value", "number"]

df = spark.createDataFrame(data, columns)
df.cache()
#df.select("name", abs("value").alias("absolute_value")).show()
#df.select("name", ceil("value").alias("ceil_value")).show()
df.select("name", floor("value").alias("floor_value")).show()
