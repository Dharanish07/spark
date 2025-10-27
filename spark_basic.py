from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder \
    .appName("SimpleSparkExample") \
    .master("local[*]") \
    .getOrCreate()

# Create a simple DataFrame
data = [
    (1, "John", 25),
    (2, "Jane", 30),
    (3, "Bob", 35),
    (4, "Alice", 28)
]
columns = ["id", "name", "age"]

df = spark.createDataFrame(data, columns)

# Show the DataFrame
print("Original DataFrame:")
df.show()

# Perform some basic operations
# Filter people over 30
print("\nPeople over 30:")
df.filter(col("age") > 30).show()

# Calculate average age
print("\nAverage age:")
avg_age = df.agg({"age": "avg"}).collect()[0][0]
print(f"Average age: {avg_age}")

# Stop the Spark session
spark.stop()
