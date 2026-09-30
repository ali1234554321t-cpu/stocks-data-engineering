from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "stock-prices",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    group_id="stock-consumer-group",
)

print("Waiting for messages...")

for message in consumer:
    print("Received:", message.value.decode())
