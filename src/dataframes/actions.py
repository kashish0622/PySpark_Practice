from src.dataframes.dataframe import df
df.select("name").show()
df.filter(df.age == 20).show()
df.limit(2).show()
df.sort("name").show()
df.where(df.age > 22).show()
df.withColumn("age", df.age / 2).show()


