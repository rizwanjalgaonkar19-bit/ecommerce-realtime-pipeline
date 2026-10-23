from datetime import datetime
from src import schemas

stream_fields = {
    "orders": schemas.order_fields,
    "customers": schemas.customer_fields,
    "products": schemas.product_fields,
    "clickstream": schemas.click_fields,
}

# duplicate pakadne ke liye har stream ki unique key
key_fields = {
    "orders": ["order_id", "product_id"],
    "customers": ["customer_id"],
    "products": ["product_id"],
    "clickstream": ["event_id"],
}

seen_keys = set()


def check_fields(stream, record):
    errors = []
    for name, wanted_type in stream_fields[stream].items():
        value = record.get(name)
        if value is None or value == "":
            errors.append("missing: " + name)
            continue
        if wanted_type == float:
            wanted_type = (int, float)
        if not isinstance(value, wanted_type):
            errors.append("wrong_type: " + name)
    return errors


def check_rules(stream, record):
    errors = []

    if stream == "orders":
        if record["quantity"] <= 0:
            errors.append("rule: quantity must be positive")
        if record["unit_price"] <= 0:
            errors.append("rule: unit_price must be positive")
        if abs(record["quantity"] * record["unit_price"] - record["total_amount"]) > 0.01:
            errors.append("rule: total_amount does not match")
        if record["order_status"] not in schemas.order_status_list:
            errors.append("rule: invalid order_status")
        if record["payment_method"] not in schemas.payment_list:
            errors.append("rule: invalid payment_method")

    if stream == "products" and record["weight_g"] < 0:
        errors.append("rule: weight cannot be negative")

    if stream == "clickstream":
        if record["page"] not in schemas.page_list:
            errors.append("rule: invalid page")
        if record["action"] not in schemas.action_list:
            errors.append("rule: invalid action")

    if stream in ("orders", "clickstream"):
        try:
            datetime.strptime(record["timestamp"], "%Y-%m-%d %H:%M:%S")
        except ValueError:
            errors.append("bad_timestamp: timestamp")

    return errors


def check_duplicate(stream, record):
    key = (stream,) + tuple(record[f] for f in key_fields[stream])
    if key in seen_keys:
        return ["duplicate: " + stream + " record"]
    seen_keys.add(key)
    return []


def validate(stream, record):
    errors = check_fields(stream, record)
    if errors:
        return errors
    errors = check_rules(stream, record)
    if errors:
        return errors
    return check_duplicate(stream, record)