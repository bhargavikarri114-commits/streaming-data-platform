from kafka import KafkaProducer
import json
import time

from data_generator import generate_order

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

order_id = 1001

while True:
    order = generate_order(order_id)
    producer.send('orders', value=order)
    print(f"Sent order: {order}")
    order_id += 1
    time.sleep(2)  # Simulate delay between orders