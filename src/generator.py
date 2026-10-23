import random
import uuid
from datetime import datetime, timedelta


def make_clicks(customer_ids, count):
    clicks = []
    time = datetime(2018, 1, 1, 10, 0, 0)

    for i in range(count):
        action = random.choices(
            ["view", "add_to_cart", "purchase"], weights=[70, 20, 10]
        )[0]

        if action == "view":
            page = random.choice(["home", "category", "product"])
        elif action == "add_to_cart":
            page = "cart"
        else:
            page = "checkout"

        time = time + timedelta(seconds=random.randint(1, 5))

        click = {
            "event_id": str(uuid.uuid4()),
            "customer_id": random.choice(customer_ids),
            "session_id": "s" + str(random.randint(1, 5000)),
            "page": page,
            "action": action,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        }
        clicks.append(click)

    return clicks