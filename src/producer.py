import json
import time
from kafka import KafkaProducer
from src import loader, generator

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
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
    # pehle 1000-1000 records bhej chuke hain, ab baaki bhejte hain
    orders = loader.load_orders()[1000:]
    customers = loader.load_customers()[1000:]
    products = loader.load_products()[1000:]

    all_customer_ids = [c["customer_id"] for c in loader.load_customers()]
    clicks = generator.make_clicks(all_customer_ids, 20000)

    delay = 0   # 0 matlab sabse tez, stream dheemi karni ho toh 0.001 jaisa daalo

    send_records("customers", customers, delay)
    send_records("products", products, delay)
    send_records("orders", orders, delay)
    send_records("clickstream", clicks, delay)