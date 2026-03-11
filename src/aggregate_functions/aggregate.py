from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.appName("NumericFunctions")\
    .master("local[*]")\
    .config("spark.driver.memory","3g")\
    .config("spark.executor.memory","2g")\
    .config("spark.executors.cores", "2")\
    .getOrCreate()
print("SparkSession created for numeric functions")

data = [
    (101, "Ayush", "IT", 50000),
    (102, "Riya", "HR", 45000),
    (103, "Karan", "IT", 60000),
    (104, "Sneha", "Finance", 55000),
    (105, "Rahul", "IT", 52000)
]

columns = ["emp_id", "name", "department", "salary"]

df = spark.createDataFrame(data, columns)
df.cache()
