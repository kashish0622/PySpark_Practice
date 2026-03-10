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
(101, "Laptop", 55000.75, 2, 5.5, 12000),
(102, "Mobile", 25000.50, 1, 10.0, 5000),
(103, "Tablet", 18000.20, 3, 7.5, -2000),
(104, "Camera", 32000.90, 1, 12.0, 8000),
(105, "Headphones", 4500.40, 4, 3.0, 1500),
(106, "Speaker", 6200.60, 2, 6.0, -500),
(107, "Smartwatch", 15000.30, 1, 8.0, 3000),
(108, "Monitor", 21000.80, 2, 4.5, 4000),
(109, "Keyboard", 1800.25, 5, 2.0, 600),
(110, "Mouse", 900.90, 6, 1.5, -200)
]

columns = ["order_id","product","price","quantity","discount","profit"]

df = spark.createDataFrame(data, columns)

#df.show()
#df.select(sum("price")).show()
#df.select(avg("discount")).show()
#df.select(min("profit")).show()
#df.select(max("profit")).show()

