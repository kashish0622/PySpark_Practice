from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.appName("Mathematical_functions")\
    .master("local[*]")\
    .config("spark.driver.memory", "5g")\
    .config("spark.executor.memory", "3g")\
    .config("spark.executor.cores", "3")\
    .getOrCreate()
print("SparkSession created for Mathematical_functions")

