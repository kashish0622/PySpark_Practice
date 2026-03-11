from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.appName("Date_and_Time_Functions")\
    .master("local[*]").config("spark.executor.memory", "5g")\
    .config("spark.executor.cores", "3")\
    .config("spark.driver.memory", "2g")\
    .getOrCreate()
print("Spark Session for DateandTime_Functions Created Successfully")

data = [
    (101, "Ayush", "2024-01-15 10:30:45"),
    (102, "Riya", "2023-07-21 14:20:10"),
    (103, "Karan", "2022-12-05 08:15:30"),
    (104, "Sneha", "2024-03-10 19:45:00"),
    (105, "Rahul", "2021-11-25 06:10:55")
]

columns = ["id", "name", "login_time"]

df = spark.createDataFrame(data, columns)
df.cache()
#df.select(current_time()).show()
df.select(current_date()).show()


