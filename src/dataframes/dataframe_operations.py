from src.dataframes.dataframe import df
df.select("name", "age").show()
df.filter(df.age == 20).show()
df.groupBy("name").count().show()