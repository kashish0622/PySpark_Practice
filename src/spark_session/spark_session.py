from pyspark.sql import SparkSession
spark = SparkSession\
    .builder\
    .appName("Pyspark_Practice")\
    .master("local[*]")\
    .config("spark.driver.memory",'2g')\
    .config("spark.executor.memory",'3g')\
    .config("spark.executor.cores", 3)\
    .getOrCreate()
print("Spark session created successfully for pyspark practice")