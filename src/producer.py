import json
import time
from kafka import KafkaProducer
from src import loader, generator
from config import settings

producer = KafkaProducer(
    bootstrap_servers=settings.kafka_server,
    value_serializer=lambda x: json.dumps(x).encode("utf-8"),
)


def send_records(topic, records, delay=0):
    for record in records:
        producer.send(topic, record)
        if delay > 0:
            time.sleep(delay)
    producer.flush()
    print(topic, "-", len(records), "records sent")


if __name__ == "__main__":
    # poora dataset bhejta hai (naye, khaali Kafka ke liye)
    orders = loader.load_orders()
    customers = loader.load_customers()
    products = loader.load_products()

    customer_ids = [c["customer_id"] for c in customers]
    clicks = generator.make_clicks(customer_ids, 21000)

    delay = 0   # 0 matlab sabse tez

    send_records("customers", customers, delay)
    send_records("products", products, delay)
    send_records("orders", orders, delay)
    send_records("clickstream", clicks, delay)