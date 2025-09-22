from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count, avg, min, max, when
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, TimestampType

# Création de la session Spark
spark = SparkSession.builder \
    .appName("WeatherAggregates") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Schéma du flux transformé
schema = StructType([
    StructField("event_time", TimestampType()),
    StructField("temperature", DoubleType()),
    StructField("windspeed", DoubleType()),
    StructField("wind_alert_level", StringType()),
    StructField("heat_alert_level", StringType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType())
])

# Lecture du flux Kafka
weather_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "weather_transformed") \
    .option("startingOffsets", "latest") \
    .load()

json_df = weather_df.select(from_json(col("value").cast("string"), schema).alias("data"))
flat_df = json_df.select(
    col("data.event_time").alias("event_time"),
    col("data.temperature"),
    col("data.windspeed"),
    col("data.wind_alert_level"),
    col("data.heat_alert_level"),
    col("data.latitude"),
    col("data.longitude")
)

# Sliding window de 5 minutes, glissement chaque minute
windowed_df = flat_df.withWatermark("event_time", "10 minutes") \
    .groupBy(
        window(col("event_time"), "5 minutes", "1 minute"),
        col("wind_alert_level"),
        col("heat_alert_level")
    ) \
    .agg(
        count(when((col("wind_alert_level") == "level_1") | (col("wind_alert_level") == "level_2"), True)).alias("wind_alerts_count"),
        count(when((col("heat_alert_level") == "level_1") | (col("heat_alert_level") == "level_2"), True)).alias("heat_alerts_count"),
        avg("temperature").alias("avg_temp"),
        min("temperature").alias("min_temp"),
        max("temperature").alias("max_temp")
    )

# Affichage en temps réel
query = windowed_df.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()
