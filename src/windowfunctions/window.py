from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("WindowFunctions")\
    .master("local[*]")\
    .config("spark.driver.memory","3g")\
    .config("spark.executor.memory","2g")\
    .config("spark.executors.cores", "2")\
    .getOrCreate()
print("SparkSession created for window functions")

data = [
    (101, "Ayush", "IT", 50000),
    (102, "Riya", "HR", 45000),
    (103, "Karan", "IT", 60000),
    (104, "Sneha", "HR", 48000),
    (105, "Rahul", "IT", 52000)
]

columns = ["emp_id", "name", "department", "salary"]

df = spark.createDataFrame(data, columns)
df.show()

window_spec = Window.partitionBy("department").orderBy(desc("salary"))
df.withColumn(
    "row_number",
    row_number().over(window_spec)
).show()

df.withColumn(
    "rank",
    rank().over(window_spec)
).show()

df.withColumn(
    "dense_rank",
    dense_rank().over(window_spec)
).show()
