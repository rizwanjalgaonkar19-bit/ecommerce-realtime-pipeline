import pandas as pd

raw_folder = "data/raw/"


def load_orders():
    orders = pd.read_csv(raw_folder + "olist_orders_dataset.csv")
    items = pd.read_csv(raw_folder + "olist_order_items_dataset.csv")
    payments = pd.read_csv(raw_folder + "olist_order_payments_dataset.csv")

    # ek order mein same product kai baar ho sakta hai, usse quantity bana lete hain
    items = items.groupby(["order_id", "product_id"]).agg(
        quantity=("order_item_id", "count"),
        unit_price=("price", "mean"),
    ).reset_index()

    # har order ka pehla payment method hi lete hain
    payments = payments.sort_values("payment_sequential")
    payments = payments.drop_duplicates("order_id")
    payments = payments[["order_id", "payment_type"]]

    data = orders.merge(items, on="order_id")
    data = data.merge(payments, on="order_id", how="left")

    data["total_amount"] = (data["quantity"] * data["unit_price"]).round(2)
    data = data.rename(columns={
        "payment_type": "payment_method",
        "order_purchase_timestamp": "timestamp",
    })

    data = data[["order_id", "customer_id", "product_id", "quantity", "unit_price",
                 "total_amount", "order_status", "payment_method", "timestamp"]]
    data = data.sort_values("timestamp")

    # khaali values ko None bana dete hain
    data = data.astype(object).where(data.notna(), None)
    return data.to_dict("records")


def load_customers():
    customers = pd.read_csv(raw_folder + "olist_customers_dataset.csv")
    customers = customers.rename(columns={
        "customer_city": "city",
        "customer_state": "state",
    })
    customers = customers[["customer_id", "customer_unique_id", "city", "state"]]
    customers = customers.astype(object).where(customers.notna(), None)
    return customers.to_dict("records")


def load_products():
    products = pd.read_csv(raw_folder + "olist_products_dataset.csv")
    products = products.rename(columns={
        "product_category_name": "category",
        "product_weight_g": "weight_g",
    })
    products = products[["product_id", "category", "weight_g"]]
    products = products.astype(object).where(products.notna(), None)
    return products.to_dict("records")