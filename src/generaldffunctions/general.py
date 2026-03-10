#created spark session
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("General_Functions").master("local[*]").config("spark.executor.memory", "5g").config("spark.executor.cores", "3").config("spark.driver.memory", "2g").getOrCreate()
print("Spark Session for General_Functions Created Successfully")

