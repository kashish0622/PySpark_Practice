from pyspark.sql.types import IntegerType, StringType

from src.spark_session.spark_session import spark
from pyspark.sql.types import *
schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer_name", StringType(), True),
    StructField("product", StringType(), True),
    StructField("category", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("price", IntegerType(), True),
    StructField("city", StringType(), True),
])

df = spark.read\
    .format("csv")\
    .option("header", "true")\
    .schema(schema)\
    .load("sales.csv")
df.printSchema()
df.show()
