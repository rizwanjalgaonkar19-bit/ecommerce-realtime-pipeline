from src import validator

good = {
    "order_id": "o1", "customer_id": "c1", "product_id": "p1",
    "quantity": 2, "unit_price": 10.0, "total_amount": 20.0,
    "order_status": "delivered", "payment_method": "credit_card",
    "timestamp": "2018-01-01 10:00:00",
}


def make(order_id, **changes):
    record = dict(good)
    record["order_id"] = order_id
    record.update(changes)
    return record


assert validator.validate("orders", good) == []
print("sahi record: pass")

assert validator.validate("orders", good)[0].startswith("duplicate")
print("duplicate: pass")

assert validator.validate("orders", make("o2", payment_method=None))[0].startswith("missing")
print("missing field: pass")

assert validator.validate("orders", make("o3", quantity="five"))[0].startswith("wrong_type")
print("wrong type: pass")

assert validator.validate("orders", make("o4", total_amount=99.0))[0].startswith("rule")
print("total mismatch: pass")

assert validator.validate("orders", make("o5", order_status="flying"))[0].startswith("rule")
print("bad status: pass")

assert validator.validate("orders", make("o6", timestamp="kal subah"))[0].startswith("bad_timestamp")
print("bad timestamp: pass")

print("saare tests pass")