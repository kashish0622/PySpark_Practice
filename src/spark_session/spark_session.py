from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("DataEngineeringApp") \
    .master("local[*]") \
    .config("spark.executor.memory", "2g") \
    .config("spark.driver.memory", "1g") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()

print("Spark Session Created Successfully")

