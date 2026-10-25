import os
import sys
import time

os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] = "C:\\hadoop\\bin;" + os.environ["PATH"]
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, to_date
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

topic = sys.argv[1]
out_folder = sys.argv[2]

spark = (
    SparkSession.builder
    .master("local[2]")
    .appName("benchmark")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

order_schema = StructType([
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("product_id", StringType()),
    StructField("quantity", IntegerType()),
    StructField("unit_price", DoubleType()),
    StructField("total_amount", DoubleType()),
    StructField("order_status", StringType()),
    StructField("payment_method", StringType()),
    StructField("timestamp", StringType()),
])

kafka_data = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", topic)
    .option("startingOffsets", "earliest")
    .load()
)

orders = kafka_data.select(
    from_json(col("value").cast("string"), order_schema).alias("data")
).select("data.*")

clean_orders = (
    orders
    .dropna(subset=["order_id", "customer_id", "product_id", "payment_method"])
    .filter(col("quantity") > 0)
    .filter(col("unit_price") > 0)
    .dropDuplicates(["order_id", "product_id"])
    .withColumn("order_time", to_timestamp(col("timestamp")))
    .withColumn("order_date", to_date(col("order_time")))
)

start = time.perf_counter()
query = (
    clean_orders.writeStream.format("parquet")
    .option("path", out_folder + "/data")
    .option("checkpointLocation", out_folder + "/checkpoint")
    .trigger(availableNow=True)
    .start()
)
query.awaitTermination()
seconds = time.perf_counter() - start

rows = spark.read.parquet(out_folder + "/data").count()
print("PROCESS_SECONDS", seconds)
print("ROWS", rows)
spark.stop()