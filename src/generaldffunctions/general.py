#created spark session
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("General_Functions").master("local[*]").config("spark.executor.memory", "5g").config("spark.executor.cores", "3").config("spark.driver.memory", "2g").getOrCreate()
print("Spark Session for General_Functions Created Successfully")

# adding data

data = [
(101,"Rahul Sharma","Laptop","Electronics",1,75000,"Delhi"),
(102,"Priya Singh","Mobile","Electronics",2,25000,"Mumbai"),
(103,"Aman Verma","Shoes","Fashion",3,2000,"Bangalore"),
(104,"Neha Gupta","Watch","Accessories",1,5000,"Delhi"),
(105,"Rohan Das","Tablet","Electronics",2,30000,"Kolkata"),
(106,"Simran Kaur","Handbag","Fashion",1,3500,"Chandigarh"),
(107,"Arjun Mehta","Headphones","Electronics",4,1500,"Pune"),
(108,"Kavita Joshi","Dress","Fashion",2,2500,"Jaipur"),
(109,"Mohit Agarwal","Camera","Electronics",1,45000,"Delhi"),
(110,"Sneha Patel","Sunglasses","Accessories",3,1200,"Ahmedabad")
]

columns = ["order_id","customer_name","product","category","quantity","price","city"]

df = spark.createDataFrame(data, schema=columns)
print(df.collect())
df.show()