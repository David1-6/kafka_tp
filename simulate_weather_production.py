import time
import sys
import subprocess

# Liste des villes à simuler

CITIES = [
    ("Paris", "France"),
    ("Lyon", "France"),
    ("Orleans", "France"),
    ("Limoges", "France"),
    ("Toulouse", "France"),
    ("Bruxelles", "Belgique"),
    ("Berlin", "Allemagne"),
    ("Madrid", "Espagne"),
    ("Rome", "Italie"),
    ("New York", "USA"),
    ("Los Angeles", "USA"),
    ("Chicago", "USA"),
    ("Miami", "USA"),
    ("San Francisco", "USA"),
]

# Nombre de mesures par ville
N = 5
# Délai entre chaque mesure (secondes)
DELAY = 10
# Topic Kafka
TOPIC = "weather_transformed"

for i in range(N):
    print(f"Envoi de la mesure {i+1}/{N} pour chaque ville...")
    for city, country in CITIES:
        cmd = [
            "python", "/workspace/current_weather.py", city, country, TOPIC
        ]
        print(f"  -> {city}, {country}")
        subprocess.run(cmd)
    if i < N-1:
        print(f"Attente {DELAY} secondes...")
        time.sleep(DELAY)
print("Simulation terminée. Vous pouvez relancer la visualisation.")
