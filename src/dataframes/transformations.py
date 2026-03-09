from src.dataframes.dataframe import df
df.select("name")
df.filter(df.age == 20)
df.limit(2)
df.sort("name")
df.where(df.age > 22)
df.withColumn("age", df.age / 2)

