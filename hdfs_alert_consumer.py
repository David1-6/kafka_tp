import json
import sys
from kafka import KafkaConsumer
import os

if len(sys.argv) != 2:
    print("Usage: python hdfs_alert_consumer.py <topic>")
    sys.exit(1)

topic = sys.argv[1]

consumer = KafkaConsumer(
    topic,
    bootstrap_servers=['kafka:9092'],
    auto_offset_reset='earliest',
    group_id='hdfs-alert-consumer',
    value_deserializer=lambda m: m.decode('utf-8')
)

for msg in consumer:
    try:
        data = json.loads(msg.value)
        country = data.get('pays', 'unknown')
        city = data.get('ville', 'unknown')
        local_path = f"alerts/{country}/{city}"
        os.makedirs(local_path, exist_ok=True)
        local_file = f"{local_path}/alerts.json"
        with open(local_file, 'a', encoding='utf-8') as f:
            f.write(json.dumps(data, ensure_ascii=False) + "\n")
        print(f"Alerte sauvegardée localement dans {local_file}")
        # Automatisation de la copie dans HDFS
        hdfs_dir = f"/hdfs-data/{country}/{city}"
        hdfs_file = f"{hdfs_dir}/alerts.json"
        # Crée le dossier dans HDFS si besoin
        os.system(f"hdfs dfs -mkdir -p {hdfs_dir}")
        # Copie le fichier dans HDFS (remplace s'il existe)
        os.system(f"hdfs dfs -put -f {local_file} {hdfs_file}")
        print(f"Alerte sauvegardée dans HDFS : {hdfs_file}")
    except Exception as e:
        print(f"Erreur lors du traitement du message : {e}")
