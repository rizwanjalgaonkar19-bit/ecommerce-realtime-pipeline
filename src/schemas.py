
order_fields = {
    "order_id": str,
    "customer_id": str,
    "product_id": str,
    "quantity": int,
    "unit_price": float,
    "total_amount": float,
    "order_status": str,
    "payment_method": str,
    "timestamp": str,
}

customer_fields = {
    "customer_id": str,
    "customer_unique_id": str,
    "city": str,
    "state": str,
}

product_fields = {
    "product_id": str,
    "category": str,
    "weight_g": float,
}

click_fields = {
    "event_id": str,
    "customer_id": str,
    "session_id": str,
    "page": str,
    "action": str,
    "timestamp": str,
}

# allowed values (validator inhe check karega)
order_status_list = ["created", "approved", "invoiced", "processing",
                     "shipped", "delivered", "canceled", "unavailable"]
payment_list = ["credit_card", "boleto", "voucher", "debit_card", "not_defined"]
page_list = ["home", "category", "product", "cart", "checkout"]
action_list = ["view", "add_to_cart", "purchase"]