from kafka import KafkaProducer
import json

# Connect Python to Kafka
producer = KafkaProducer(
    bootstrap_servers='localhost:9092', # Kafka Broker
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
# Kafka stores bytes, not Python dictionaries, 
# so we need to serialize the data before sending it to Kafka. 
# We use JSON serialization here.

order = {
    "order_id": 1001,
    "customer_id": 501,
    "product": "Laptop",
    "quantity": 2,
    "price": 60000
}

producer.send('orders', order) # Send Event sends the order into the orders topic.
producer.flush()
print("Order sent successfully!")