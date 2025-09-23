import sys
import requests
import json
from kafka import KafkaProducer
from datetime import datetime

# Usage: python produce_weather_history.py <city> <country> <latitude> <longitude> <topic>

if len(sys.argv) < 6:
    print("Usage: python produce_weather_history.py <city> <country> <latitude> <longitude> <topic>")
    sys.exit(1)

city = sys.argv[1]
country = sys.argv[2]
latitude = sys.argv[3]
longitude = sys.argv[4]
topic = sys.argv[5]

# Période sur 10 ans
end_date = datetime.now().date()
start_date = end_date.replace(year=end_date.year - 10)

# API Open-Meteo Archive
url = (
    f"https://archive-api.open-meteo.com/v1/archive?latitude={latitude}&longitude={longitude}"
    f"&start_date={start_date}&end_date={end_date}"
    f"&daily=temperature_2m_max,temperature_2m_min,precipitation_sum,windspeed_10m_max,weathercode"
    f"&timezone=Europe/Paris"
)

print(f"Téléchargement des données historiques pour {city}, {country} sur 10 ans...")
resp = requests.get(url)
if resp.status_code != 200:
    print(f"Erreur API: {resp.status_code}")
    sys.exit(1)
data = resp.json()

# Préparer le message brut
message = {
    "city": city,
    "country": country,
    "latitude": latitude,
    "longitude": longitude,
    "history": data
}

producer = KafkaProducer(
    bootstrap_servers=["kafka:9092"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)
producer.send(topic, message)
producer.flush()
print(f"Données historiques envoyées sur le topic {topic}.")
