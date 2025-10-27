from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, IntegerType

# 1️⃣ Create Spark Session
spark = SparkSession.builder \
    .appName("KafkaOrdersConsumer") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2️⃣ Define Kafka source
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "orders") \
    .option("startingOffsets", "latest") \
    .load()

# 3️⃣ Define schema of the JSON message
order_schema = StructType() \
    .add("order_id", StringType()) \
    .add("user", StringType()) \
    .add("item", StringType()) \
    .add("quantity", IntegerType())

# 4️⃣ Convert Kafka binary 'value' to JSON fields
orders_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str")

parsed_df = orders_df.select(
    from_json(col("json_str"), order_schema).alias("data")
).select("data.*")

# 5️⃣ Write stream to console
query = parsed_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

# 6️⃣ Keep streaming
query.awaitTermination()
