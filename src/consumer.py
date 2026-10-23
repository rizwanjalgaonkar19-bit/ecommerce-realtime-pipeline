import json
from kafka import KafkaConsumer

topics = ["orders", "customers", "products", "clickstream"]

consumer = KafkaConsumer(
    *topics,
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    group_id="test-group",
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    consumer_timeout_ms=5000,
)

count = {}
sample = {}

for message in consumer:
    topic = message.topic
    count[topic] = count.get(topic, 0) + 1
    if topic not in sample:
        sample[topic] = message.value

for topic in topics:
    print(topic, "-", count.get(topic, 0), "messages")
    print("   pehla record:", sample.get(topic))