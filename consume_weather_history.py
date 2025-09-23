import sys
import os
import json
from kafka import KafkaConsumer
import subprocess

# Usage: python consume_weather_history.py <topic>

if len(sys.argv) < 2:
    print("Usage: python consume_weather_history.py <topic>")
    sys.exit(1)

topic = sys.argv[1]

consumer = KafkaConsumer(
    topic,
    bootstrap_servers=["kafka:9092"],
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='weather_history_group'
)

for msg in consumer:
    data = msg.value
    city = data.get("city", "unknown")
    country = data.get("country", "unknown")
    # Dossier local temporaire
    local_dir = f"/tmp/weather_history_raw/{country}/{city}"
    os.makedirs(local_dir, exist_ok=True)
    local_path = os.path.join(local_dir, "weather_history_raw.json")
    with open(local_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    # Dossier HDFS cible
    hdfs_dir = f"/hdfs-data/{country}/{city}/weather_history_raw"
    subprocess.run(f"hdfs dfs -mkdir -p {hdfs_dir}", shell=True)
    hdfs_path = f"{hdfs_dir}/weather_history_raw.json"
    subprocess.run(f"hdfs dfs -put -f {local_path} {hdfs_path}", shell=True)
    print(f"Historique météo sauvegardé dans HDFS : {hdfs_path}")
    # Si on ne veut qu'un message, décommentez la ligne suivante :
    # break
