import sys
import requests
from kafka import KafkaProducer
import json


# Utilisation : python current_weather.py <ville> <pays> [topic]
if len(sys.argv) not in (3, 4):
    print("Usage: python current_weather.py <ville> <pays> [topic]")
    sys.exit(1)

ville = sys.argv[1]
pays = sys.argv[2]
topic = sys.argv[3] if len(sys.argv) == 4 else "weather_stream"

# Appel à l'API de géocodage Open-Meteo pour obtenir latitude/longitude
geocode_url = f"https://geocoding-api.open-meteo.com/v1/search?name={ville}&country={pays}&count=1&language=fr&format=json"
geocode_resp = requests.get(geocode_url)
if geocode_resp.status_code != 200 or not geocode_resp.json().get("results"):
    print(f"Erreur lors de la géolocalisation de {ville}, {pays}")
    sys.exit(1)

geo = geocode_resp.json()["results"][0]
latitude = geo["latitude"]
longitude = geo["longitude"]

# Appel à l'API Open-Meteo pour la météo
url = f"https://api.open-meteo.com/v1/forecast?latitude={latitude}&longitude={longitude}&current_weather=true"
response = requests.get(url)
if response.status_code != 200:
    print(f"Erreur lors de la requête API météo: {response.status_code}")
    sys.exit(1)

weather_data = response.json()
weather_data["ville"] = ville
weather_data["pays"] = pays

# Envoi dans Kafka
producer = KafkaProducer(
    bootstrap_servers=["kafka:9092"],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
producer.send(topic, weather_data)
producer.flush()
print(f"Données météo envoyées pour {ville}, {pays} (lat={latitude}, lon={longitude}) sur le topic {topic}")
