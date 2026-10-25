import json
import os
import platform
import shutil
import subprocess
import sys
import time

import pandas as pd
from kafka import KafkaProducer, KafkaConsumer
from src import loader, validator

sizes = [10000, 50000, 100000]
runs = 3

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda x: json.dumps(x).encode("utf-8"),
)


def produce(topic, records):
    producer.partitions_for(topic)   # topic pehle bana lo, uska time na gine
    start = time.perf_counter()
    for record in records:
        producer.send(topic, record)
    producer.flush()
    return time.perf_counter() - start


def validate_topic(topic):
    validator.seen_keys.clear()
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers="localhost:9092",
        auto_offset_reset="earliest",
        group_id=None,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        consumer_timeout_ms=5000,
    )
    first = None
    last = None
    total = 0
    valid = 0
    for message in consumer:
        if first is None:
            first = time.perf_counter()
        errors = validator.validate("orders", message.value)
        total += 1
        if not errors:
            valid += 1
        last = time.perf_counter()
    consumer.close()
    return total, valid, last - first


def run_spark(topic):
    out_folder = "C:/pipeline-data/bench/" + topic
    shutil.rmtree(out_folder, ignore_errors=True)
    result = subprocess.run(
        [sys.executable, "-m", "src.spark_bench", topic, out_folder],
        capture_output=True, text=True,
    )
    seconds = None
    rows = None
    for line in result.stdout.splitlines():
        if line.startswith("PROCESS_SECONDS"):
            seconds = float(line.split()[1])
        if line.startswith("ROWS"):
            rows = int(line.split()[1])
    if seconds is None:
        print("spark fail hua:", result.stderr[-800:])
    return seconds, rows


if __name__ == "__main__":
    all_orders = loader.load_orders()
    print("python", platform.python_version(), "| cpu cores:", os.cpu_count())

    results = []
    for size in sizes:
        records = all_orders[:size]
        for run in range(1, runs + 1):
            topic = "bench_" + str(size) + "_run" + str(run) + "_" + str(int(time.time()))

            produce_seconds = produce(topic, records)
            total, valid, validate_seconds = validate_topic(topic)
            spark_seconds, spark_rows = run_spark(topic)

            row = {
                "size": size,
                "run": run,
                "producer_rec_per_sec": round(size / produce_seconds),
                "consumed": total,
                "valid": valid,
                "validator_rec_per_sec": round(total / validate_seconds),
                "spark_seconds": round(spark_seconds, 2) if spark_seconds else None,
                "spark_rows": spark_rows,
                "spark_rec_per_sec": round(size / spark_seconds) if spark_seconds else None,
            }
            results.append(row)
            print(row)

    df = pd.DataFrame(results)
    df.to_csv("results/benchmark_results.csv", index=False)

    print("\n---- summary (mean and std of 3 runs) ----")
    columns = ["producer_rec_per_sec", "validator_rec_per_sec", "spark_rec_per_sec"]
    print(df.groupby("size")[columns].agg(["mean", "std"]).round(0).to_string())