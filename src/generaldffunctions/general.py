#created spark session
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("General_Functions").master("local[*]").getOrCreate()
print("Spark Session for General_Functions Created Successfully")
