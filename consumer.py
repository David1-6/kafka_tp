import sys
from kafka import KafkaConsumer

if len(sys.argv) != 2:
    print("Usage: python consumer.py <topic>")
    sys.exit(1)

topic = sys.argv[1]

# Connexion au broker Kafka
consumer = KafkaConsumer(
    topic,
    bootstrap_servers=['localhost:29092'],
    auto_offset_reset='earliest',
    group_id='tp-consumer-group',
    value_deserializer=lambda m: m.decode('utf-8')
)

print(f"En attente de messages sur le topic '{topic}'...")
for message in consumer:
    print(f"Message reçu: {message.value}")
