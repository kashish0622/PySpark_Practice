from pyspark.sql import SparkSession

def create_DataFrame():

    # Create Spark Session
    spark = SparkSession.builder \
        .appName("PySpark") \
        .master("local[*]") \
        .getOrCreate()

    print("Spark Session Created Successfully")

    # Creating data
    data = [
        (1, "Kashish", 21, "Data Engineering"),
        (2, "Rahul", 23, "AI"),
        (3, "Ananya", 20, "Data Science"),
        (4, "Aman", 22, "Cyber Security")
    ]

    columns = ["id", "name", "age", "department"]

    # Create DataFrame
    df = spark.createDataFrame(data, columns)

    return spark, df
