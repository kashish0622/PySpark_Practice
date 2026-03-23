from src.spark_session.spark_session import spark
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True)
])

# Step 3: Create Data
data = [
    (1, "Kashish", 21),
    (2, "Rahul", 25),
    (3, "Anjali", 23)
]

df = spark.createDataFrame(data, schema)
df.write.mode("overwrite").parquet("data/parquet_data")

df_parquet = spark.read.parquet("data/parquet_data")

df_parquet.show()
df_parquet.printSchema()