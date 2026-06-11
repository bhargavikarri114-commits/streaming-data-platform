import random
from datetime import datetime

products = [
    ("Laptop", "Electronics"),
    ("Phone", "Electronics"),
    ("Monitor", "Electronics"),
    ("Keyboard", "Accessories"),
    ("Mouse", "Accessories"),
    ("Printer", "Office"),
    ("Tablet", "Electronics"),
    ("Webcam", "Accessories"),
    ("Speaker", "Electronics"),
    ("USB Drive", "Accessories")
]

def generate_order(order_id):

    product, category = random.choice(products)
    order = {
        "order_id": order_id,
        "customer_id": random.randint(1000, 9999),
        "product": product,
        "category": category,
        "quantity": random.randint(1, 5),
        "price": random.randint(500, 75000),
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    return order