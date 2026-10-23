import time

stats = {
    "total_records": 0,
    "valid_records": 0,
    "invalid_records": 0,
    "duplicate_records": 0,
}
error_counts = {}
start_time = None
last_time = None


def update(errors):
    global start_time, last_time
    last_time = time.time()
    if start_time is None:
        start_time = last_time

    stats["total_records"] += 1

    if not errors:
        stats["valid_records"] += 1
        return

    stats["invalid_records"] += 1
    category = errors[0].split(":")[0]
    error_counts[category] = error_counts.get(category, 0) + 1
    if category == "duplicate":
        stats["duplicate_records"] += 1


def show():
    if stats["total_records"] == 0:
        print("koi record nahi aaya")
        return

    seconds = max(last_time - start_time, 0.001)
    rate = stats["valid_records"] / stats["total_records"] * 100

    print("---- data quality metrics ----")
    for name, value in stats.items():
        print(name, ":", value)
    print("validation_rate :", round(rate, 2), "%")
    print("error counts    :", error_counts)
    print("records/second  :", round(stats["total_records"] / seconds, 1))