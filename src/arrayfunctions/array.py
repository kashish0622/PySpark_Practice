from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.appName("Array_functions")\
    .master("local[*]")\
    .config("spark.driver.memory", "5g")\
    .config("spark.executor.memory", "3g")\
    .config("spark.executor.cores", "3")\
    .getOrCreate()
print("SparkSession created for Array_functions")

data = [
    (101, "Ayush", "Python", "SQL", "Spark"),
    (102, "Riya", "Excel", "SQL", None),
    (103, "Karan", "Python", "Java", "Scala"),
    (104, "Sneha", "Python", "Spark", None),
    (105, "Rahul", "SQL", "Tableau", None)
]

columns = ["emp_id", "name", "skill_set1", "skill_set2", "skill_set3"]

df = spark.createDataFrame(data, columns)
df.cache()
#df.select(array(col("skill_set1"),col("skill_set2")).alias("skills")).show()
df.select(
    "name",
    array_contains(
        array(col("skill_set1"), col("skill_set2"), col("skill_set3")),
        "Python"
    ).alias("knows_python")
).show()