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

