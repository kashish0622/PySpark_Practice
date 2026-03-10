def test_general(spark):
    data = [
        (101, "Ayush", 22),
        (102, "Bushra", 24),
        (103, "Diana", 22)
    ]
    columns = ["roll_id","name","age"]
    df = spark.createDataFrame(data, columns)
    result = df.sort("age")
    result2 = [row.age for row in result.collect()]
    assert result2 == [22,22,24]

