# creating data frame here
spark = SparkSession.builder\
    .appName("PySpark")\
    .master("local[*]")\
    .getOrCreate()
print("Spark Session Created Successfully")
