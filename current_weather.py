import sys
import requests
from kafka import KafkaProducer
import json

if len(sys.argv) != 3:
    print("Usage: python current_weather.py <latitude> <longitude>")
    sys.exit(1)

latitude = sys.argv[1]
longitude = sys.argv[2]

# Appel à l'API Open-Meteo
url = f"https://api.open-meteo.com/v1/forecast?latitude={latitude}&longitude={longitude}&current_weather=true"
response = requests.get(url)
if response.status_code != 200:
    print(f"Erreur lors de la requête API: {response.status_code}")
    sys.exit(1)

weather_data = response.json()

# Envoi dans Kafka
topic = "weather_stream"
producer = KafkaProducer(
    bootstrap_servers=["localhost:29092"],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
producer.send(topic, weather_data)
producer.flush()
print(f"Données météo envoyées pour lat={latitude}, lon={longitude}")
