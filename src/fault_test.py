import json
import os
import time
from kafka import KafkaProducer, KafkaConsumer
from src import loader, validator, quarantine, metrics

# asli quarantine se alag folder, taaki dashboard ke numbers na badlein
quarantine.quarantine_folder = "data/fault_test/"
old_file = "data/fault_test/orders_bad.jsonl"
if os.path.exists(old_file):
    os.remove(old_file)

# har run ke liye naya topic
topic = "orders_test_" + str(int(time.time()))

# 999 saaf orders (jinka payment_method khaali nahi hai)
good = [o for o in loader.load_orders()[:1000] if o["payment_method"] is not None]
base = good[0]


def copy_of(order_id, **changes):
    new = dict(base)
    new["order_id"] = order_id
    new.update(changes)
    return new


faults = []
for i in range(20):
    faults.append(dict(good[i]))                                              # duplicate
    faults.append(copy_of("neg_" + str(i), quantity=-1,
                          total_amount=-base["unit_price"]))                  # negative quantity
    faults.append(copy_of("time_" + str(i), timestamp="not a date"))          # bad timestamp
    faults.append(copy_of("miss_" + str(i), customer_id=None))                # missing field
    faults.append(copy_of("type_" + str(i), quantity="two"))                  # wrong type
    faults.append(copy_of("total_" + str(i),
                          total_amount=base["total_amount"] + 50))            # total mismatch
    faults.append(copy_of("status_" + str(i), order_status="flying"))         # invalid status

# kafka mein bhejo (pehle saaf, phir galat)
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda x: json.dumps(x).encode("utf-8"),
)
for record in good + faults:
    producer.send(topic, record)
producer.flush()
print("sent:", len(good), "good +", len(faults), "faulty records to", topic)

# kafka se padho aur validate karo
consumer = KafkaConsumer(
    topic,
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    group_id=None,
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    consumer_timeout_ms=5000,
)

for message in consumer:
    errors = validator.validate("orders", message.value)
    metrics.update(errors)
    if errors:
        quarantine.save_bad_record("orders", message.value, errors)

metrics.show()

expected = {"duplicate": 20, "rule": 60, "bad_timestamp": 20, "missing": 20, "wrong_type": 20}
print("expected errors :", expected)
if metrics.error_counts == expected and metrics.stats["valid_records"] == len(good):
    print("fault test PASS")
else:
    print("fault test FAIL, upar ke numbers dekho")