from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("PySpark Practice") \
    .master("local[*]") \
    .config("spark.executor.memory", "2g") \
    .config("spark.driver.memory", "1g") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()

print("Spark Session Created Successfully")

from pyspark.sql import SparkSession

# Creating SparkSession
spark = SparkSession.builder \
    .appName("PySpark Practice") \
    .getOrCreate()

# Display Spark version
print("Spark Version:", spark.version)