from src.spark_session.spark_session import spark
from pyspark.sql.types import StructType, StructField, StringType

schema = StructType([
    StructField("id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("skill", StringType(), True),
    StructField("rating", StringType(), True),
])

df = spark.read.csv("data1.csv", schema=schema, header=True)
df.printSchema()
df.show()