from tokenize import String

from src.spark_session.spark_session import spark
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("address",StructType([
        StructField("city", StringType(), True),
        StructField("zip", StringType(), True),
    ]), True)
])

df = spark.read\
    .schema(schema)\
    .json("nested.json")
df.printSchema()
df.show()