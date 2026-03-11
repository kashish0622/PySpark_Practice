from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.appName("ConversionFunctions")\
    .master("local[*]")\
    .config("spark.driver.memory","3g")\
    .config("spark.executor.memory","2g")\
    .config("spark.executors.cores", "2")\
    .getOrCreate()
print("SparkSession created for conversion functions")

data = [
    (101, "Ayush", "25", "50000.50"),
    (102, "Riya", "30", "45000.75"),
    (103, "Karan", "28", "60000.20"),
    (104, "Sneha", "26", "55000.10"),
    (105, "Rahul", "32", "52000.60")
]

columns = ["emp_id", "name", "age_string", "salary_string"]

df = spark.createDataFrame(data, columns)
df.cache()
df.select("name", col("age_string").cast("int").alias("age")).show()
df.select("name", col("salary_string").cast("double").alias("salary")).show()
