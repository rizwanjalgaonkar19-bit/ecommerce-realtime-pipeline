import os

# windows ke liye settings
os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] = "C:\\hadoop\\bin;" + os.environ["PATH"]
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, to_date
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

output_folder = "C:/pipeline-data/"

spark = (
    SparkSession.builder
    .master("local[2]")
    .appName("ecommerce-streaming")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

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

customer_schema = StructType([
    StructField("customer_id", StringType()),
    StructField("customer_unique_id", StringType()),
    StructField("city", StringType()),
    StructField("state", StringType()),
])

product_schema = StructType([
    StructField("product_id", StringType()),
    StructField("category", StringType()),
    StructField("weight_g", DoubleType()),
])

click_schema = StructType([
    StructField("event_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("session_id", StringType()),
    StructField("page", StringType()),
    StructField("action", StringType()),
    StructField("timestamp", StringType()),
])


def read_topic(topic, schema):
    kafka_data = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
    )
    # kafka ka message bytes mein aata hai, JSON padh ke columns banao
    return kafka_data.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")


def write_parquet(df, folder, name):
    return (
        df.writeStream.format("parquet")
        .option("path", output_folder + folder + "/" + name)
        .option("checkpointLocation", output_folder + "checkpoints/" + folder + "_" + name)
        .trigger(availableNow=True)
        .start()
    )


orders = read_topic("orders", order_schema)
customers = read_topic("customers", customer_schema)
products = read_topic("products", product_schema)
clicks = read_topic("clickstream", click_schema)

# saaf karna: null hatao, galat values hatao, duplicate hatao, time ke columns jodo
clean_orders = (
    orders
    .dropna(subset=["order_id", "customer_id", "product_id", "payment_method"])
    .filter(col("quantity") > 0)
    .filter(col("unit_price") > 0)
    .dropDuplicates(["order_id", "product_id"])
    .withColumn("order_time", to_timestamp(col("timestamp")))
    .withColumn("order_date", to_date(col("order_time")))
)

clean_customers = (
    customers
    .dropna(subset=["customer_id", "customer_unique_id", "city", "state"])
    .dropDuplicates(["customer_id"])
)

clean_products = (
    products
    .dropna(subset=["product_id", "category", "weight_g"])
    .dropDuplicates(["product_id"])
)

clean_clicks = (
    clicks
    .dropna()
    .dropDuplicates(["event_id"])
    .withColumn("event_time", to_timestamp(col("timestamp")))
    .withColumn("event_date", to_date(col("event_time")))
)

queries = [
    write_parquet(orders, "raw", "orders"),
    write_parquet(customers, "raw", "customers"),
    write_parquet(products, "raw", "products"),
    write_parquet(clicks, "raw", "clickstream"),
    write_parquet(clean_orders, "processed", "orders"),
    write_parquet(clean_customers, "processed", "customers"),
    write_parquet(clean_products, "processed", "products"),
    write_parquet(clean_clicks, "processed", "clickstream"),
]

for q in queries:
    q.awaitTermination()

print("spark streaming khatam: 4 streams ke raw aur processed parquet likh diye")