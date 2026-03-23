from src.spark_session.spark_session import spark
from pyspark.sql.types import *
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", StringType(), True)
])

df = spark.read\
    .format("json")\
    .schema(schema)\
    .load("data.json")
df.printSchema()
df.show()