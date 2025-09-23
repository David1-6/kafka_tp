import os
import json
import subprocess
import pandas as pd
import matplotlib.pyplot as plt
from collections import Counter

# Dossier HDFS racine
HDFS_ROOT = "/hdfs-data"
LOCAL_TMP = "/tmp/hdfs_alerts"

# 1. Récupérer tous les fichiers alerts.json depuis HDFS
def fetch_hdfs_alerts():
    os.makedirs(LOCAL_TMP, exist_ok=True)
    # Liste tous les fichiers alerts.json dans HDFS
    cmd = f"hdfs dfs -find {HDFS_ROOT} -name alerts.json"
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    files = result.stdout.strip().split("\n")
    local_files = []
    for hdfs_file in files:
        if not hdfs_file:
            continue
        # Chemin local temporaire
        parts = hdfs_file.strip("/").split("/")
        country, city = parts[1], parts[2]
        local_dir = os.path.join(LOCAL_TMP, country, city)
        os.makedirs(local_dir, exist_ok=True)
        local_path = os.path.join(local_dir, "alerts.json")
        subprocess.run(f"hdfs dfs -get -f {hdfs_file} {local_path}", shell=True)
        local_files.append((country, city, local_path))
    return local_files

# 2. Charger tous les fichiers en DataFrame
def load_alerts_to_df(local_files):
    rows = []
    for country, city, path in local_files:
        try:
            with open(path, "r", encoding="utf-8") as f:
                content = f.read().strip()
                # Essayer d'abord de charger comme une liste JSON
                try:
                    data = json.loads(content)
                    if isinstance(data, dict):
                        data = [data]
                    elif isinstance(data, str):
                        # Parfois un seul objet JSON encodé en string
                        data = [json.loads(data)]
                except Exception:
                    # Sinon, essayer JSONL (un objet JSON par ligne)
                    data = []
                    for line in content.splitlines():
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            obj = json.loads(line)
                            data.append(obj)
                        except Exception:
                            continue
                for alert in data:
                    if isinstance(alert, dict):
                        alert["country"] = country
                        alert["city"] = city
                        rows.append(alert)
        except Exception as e:
            print(f"Erreur lecture {path}: {e}")
    return pd.DataFrame(rows)

# 3. Visualisations demandées
def plot_temperature(df):
    plt.figure(figsize=(10,5))
    for (country, city), group in df.groupby(["country", "city"]):
        group = group.sort_values("timestamp")
        plt.plot(pd.to_datetime(group["timestamp"]), group["temperature"], label=f"{country}-{city}")
    plt.title("Évolution de la température")
    plt.xlabel("Temps")
    plt.ylabel("Température (°C)")
    plt.legend()
    plt.tight_layout()
    plt.savefig("temperature_evolution.png")
    plt.close()

def plot_wind_speed(df):
    plt.figure(figsize=(10,5))
    for (country, city), group in df.groupby(["country", "city"]):
        group = group.sort_values("timestamp")
        plt.plot(pd.to_datetime(group["timestamp"]), group["wind_speed"], label=f"{country}-{city}")
    plt.title("Évolution de la vitesse du vent")
    plt.xlabel("Temps")
    plt.ylabel("Vitesse du vent (km/h)")
    plt.legend()
    plt.tight_layout()
    plt.savefig("wind_speed_evolution.png")
    plt.close()

def plot_alerts_by_level(df):
    # On suppose que les alertes sont dans une clé 'alerts' ou 'alert_type' et 'level'
    if "alert_type" in df.columns and "level" in df.columns:
        pivot = df.pivot_table(index="level", columns="alert_type", values="timestamp", aggfunc="count", fill_value=0)
        pivot[[c for c in ["vent", "chaleur"] if c in pivot.columns]].plot(kind="bar", stacked=True)
        plt.title("Nombre d'alertes vent et chaleur par niveau")
        plt.xlabel("Niveau d'alerte")
        plt.ylabel("Nombre d'alertes")
        plt.tight_layout()
        plt.savefig("alerts_by_level.png")
        plt.close()
    else:
        print("Colonnes 'alert_type' et 'level' absentes")

def plot_weather_code_by_country(df):
    if "weather_code" in df.columns:
        codes = df.groupby("country")["weather_code"].agg(lambda x: Counter(x).most_common(1)[0][0])
        codes.plot(kind="bar")
        plt.title("Code météo le plus fréquent par pays")
        plt.xlabel("Pays")
        plt.ylabel("Code météo le plus fréquent")
        plt.tight_layout()
        plt.savefig("weather_code_by_country.png")
        plt.close()
    else:
        print("Colonne 'weather_code' absente")

def main():
    print("Récupération des fichiers alerts.json depuis HDFS...")
    local_files = fetch_hdfs_alerts()
    print(f"{len(local_files)} fichiers récupérés.")
    df = load_alerts_to_df(local_files)
    if df.empty:
        print("Aucune donnée trouvée.")
        return
    print(f"Colonnes détectées : {list(df.columns)}")
    # Déplier la colonne current_weather si elle existe
    if "current_weather" in df.columns:
        # current_weather est une colonne d'objets (dict)
        cw = pd.json_normalize(df["current_weather"])
        cw.columns = [f"cw_{c}" for c in cw.columns]
        df = pd.concat([df, cw], axis=1)
        # Renommer pour compatibilité avec les visualisations
        if "cw_time" in df.columns:
            df = df.rename(columns={"cw_time": "timestamp"})
        if "cw_temperature" in df.columns:
            df = df.rename(columns={"cw_temperature": "temperature"})
        if "cw_windspeed" in df.columns:
            df = df.rename(columns={"cw_windspeed": "wind_speed"})
        if "cw_weathercode" in df.columns:
            df = df.rename(columns={"cw_weathercode": "weather_code"})
    print(f"Colonnes après extraction : {list(df.columns)}")
    print(f"Nombre de lignes extraites : {len(df)}")
    # Afficher un aperçu des données météo extraites
    preview_cols = [c for c in ["country", "city", "timestamp", "temperature", "wind_speed", "weather_code"] if c in df.columns]
    print("Aperçu des données extraites :")
    print(df[preview_cols].head(10))
    print("Génération des visualisations...")
    if "timestamp" in df.columns and "temperature" in df.columns:
        plot_temperature(df)
    else:
        print("Colonne 'timestamp' ou 'temperature' absente : pas de courbe température.")
    if "timestamp" in df.columns and "wind_speed" in df.columns:
        plot_wind_speed(df)
    else:
        print("Colonne 'timestamp' ou 'wind_speed' absente : pas de courbe vent.")
    plot_alerts_by_level(df)
    plot_weather_code_by_country(df)
    print("Graphiques générés (si données présentes) : temperature_evolution.png, wind_speed_evolution.png, alerts_by_level.png, weather_code_by_country.png")

if __name__ == "__main__":
    main()
