from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'orders',
    bootstrap_servers='localhost:9092',
    group_id='analytics_group',
    auto_offset_reset='latest',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("Waiting for messages...\n")

for message in consumer:

    order = message.value
    print(f"""
    Order Received
    --------------
    Order ID    : {order['order_id']}
    Customer ID : {order['customer_id']}
    Product     : {order['product']}
    Category    : {order.get('category', 'N/A')}
    Quantity    : {order['quantity']}
    Price       : {order['price']}
    Timestamp   : {order.get('timestamp', 'N/A')}
    """)