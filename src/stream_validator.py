import json
import os
from kafka import KafkaConsumer
from src import validator, quarantine, metrics
from config import settings

topics = ["orders", "customers", "products", "clickstream"]

consumer = KafkaConsumer(
    *topics,
     bootstrap_servers=settings.kafka_server,
    auto_offset_reset="earliest",
    group_id=None,
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    consumer_timeout_ms=5000,
)

# har run ke liye quarantine naye sire se shuru (warna purani lines jud jaati hain)
for stream in topics:
    old_file = quarantine.quarantine_folder + stream + "_bad.jsonl"
    if os.path.exists(old_file):
        os.remove(old_file)

for message in consumer:
    stream = message.topic
    record = message.value

    errors = validator.validate(stream, record)
    metrics.update(errors)

    if errors:
        quarantine.save_bad_record(stream, record, errors)

metrics.show()