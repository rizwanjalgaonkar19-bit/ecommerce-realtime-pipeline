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
    # abhi testing ke liye sirf pehle 1000 records bhej rahe hain
    orders = loader.load_orders()[:1000]
    customers = loader.load_customers()
    products = loader.load_products()

    customer_ids = [c["customer_id"] for c in customers]
    clicks = generator.make_clicks(customer_ids, 1000)

    send_records("customers", customers[:1000])
    send_records("products", products[:1000])
    send_records("orders", orders, delay=0.01)
    send_records("clickstream", clicks, delay=0.01)