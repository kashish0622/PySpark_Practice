# creating session here
from pyspark.sql import SparkSession
spark = SparkSession.builder\
    .appName("PySpark")\
    .master("local[*]")\
    .getOrCreate()
print("Spark Session Created Successfully")

# creation dataframe
data = [
    (1, "Kashish", 21),
    (2, "Rahul", 23),
    (3, "Ananya", 20),
    (4, "Aman", 22)
]

columns = ["id", "name", "age"]

df = spark.createDataFrame(data, columns)

df.show()