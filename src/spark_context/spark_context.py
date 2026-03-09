from pyspark import SparkConf, SparkContext

# Create Spark configuration
conf = SparkConf() \
    .setAppName("PySpark Practice") \
    .setMaster("local[*]") \
    .set("spark.executor.memory", "2g") \
    .set("spark.driver.memory", "1g")

# Create SparkContext
sc = SparkContext(conf=conf)

# Verify SparkContext
print("SparkContext Created")
print("Spark Version:", sc.version)