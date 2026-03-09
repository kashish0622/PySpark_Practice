from rdd.rdd import create_DataFrame
spark, df = create_DataFrame()

df.createOrReplaceTempView("students")

result = spark.sql("SELECT * FROM students WHERE age > 20")

result.show()