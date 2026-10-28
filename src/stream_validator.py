import json
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

for message in consumer:
    stream = message.topic
    record = message.value

    errors = validator.validate(stream, record)
    metrics.update(errors)

    if errors:
        quarantine.save_bad_record(stream, record, errors)

metrics.show()