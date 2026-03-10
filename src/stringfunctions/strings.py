from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.appName("StringFunctions")\
    .master("local[*]")\
    .config("spark.driver.memory","3g")\
    .config("spark.executor.memory","2g")\
    .config("spark.executors.cores", "2")\
    .getOrCreate()
print("SparkSession created for string functions")

data = [
(101, "  ayush bhatt  ", "delhi", "ayush123@gmail.com", "laptop", "very good product"),
(102, " BUSHRA KHAN ", "MUMBAI", "bushra.khan@gmail.com", "mobile", "excellent performance"),
(103, " diana ross ", "bangalore", "diana_ross@yahoo.com", "tablet", "average quality"),
(104, "  Rahul Sharma ", "PUNE", "rahul.s@gmail.com", "headphones", "sound quality is amazing"),
(105, " Sneha Patel  ", "ahmedabad", "sneha.patel@gmail.com", "camera", "picture quality is great"),
(106, " amit kumar ", "kolkata", "amitk@gmail.com", "smart watch", "battery life is good"),
(107, " PRIYA SINGH ", "JAIPUR", "priya.singh@yahoo.com", "laptop", "design is beautiful"),
(108, "  rohan das ", "CHENNAI", "rohan.das@gmail.com", "speaker", "sound is loud"),
(109, " Karan Mehta ", "delhi", "karan.mehta@gmail.com", "mobile", "value for money"),
(110, " ananya gupta ", "HYDERABAD", "ananya.g@gmail.com", "tablet", "fast performance")
]

columns = ["customer_id","name","city","email","product","review"]

df = spark.createDataFrame(data, columns)
df.show(truncate=False)

#df.select(upper("name")).show()
#df.select(trim("name")).show(truncate=False)
#df.select(lower("city")).show(truncate=False)
df.select("name", ltrim("name")).show()
df.select("name", rtrim("name")).show()



