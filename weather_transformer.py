from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, when
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, TimestampType
import sys

# Création de la session Spark
spark = SparkSession.builder \
    .appName("WeatherStreamTransformer") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Définition du schéma des messages météo
schema = StructType([
    StructField("current_weather", StructType([
        StructField("temperature", DoubleType()),
        StructField("windspeed", DoubleType()),
        StructField("time", StringType())
    ])),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("ville", StringType()),
    StructField("pays", StringType())
])

# Lecture du flux Kafka
weather_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "weather_stream") \
    .option("startingOffsets", "latest") \
    .load()

# Décodage et parsing JSON
json_df = weather_df.select(from_json(col("value").cast("string"), schema).alias("data"))

# Extraction des champs utiles
flat_df = json_df.select(
    col("data.current_weather.temperature").alias("temperature"),
    col("data.current_weather.windspeed").alias("windspeed"),
    col("data.current_weather.time").alias("event_time"),
    col("data.latitude"),
    col("data.longitude"),
    col("data.ville").alias("ville"),
    col("data.pays").alias("pays")
)

# Détection des alertes
result_df = flat_df.withColumn(
    "wind_alert_level",
    when(col("windspeed") < 10, "level_0")
    .when((col("windspeed") >= 10) & (col("windspeed") < 20), "level_1")
    .otherwise("level_2")
).withColumn(
    "heat_alert_level",
    when(col("temperature") < 25, "level_0")
    .when((col("temperature") >= 25) & (col("temperature") < 35), "level_1")
    .otherwise("level_2")
)

# Sérialisation en JSON
from pyspark.sql.functions import to_json, struct
output_df = result_df.withColumn("event_time", current_timestamp())
output_json_df = output_df.select(to_json(struct(*output_df.columns)).alias("value"))

# Écriture dans le topic Kafka weather_transformed
query = output_json_df.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("topic", "weather_transformed") \
    .option("checkpointLocation", "/tmp/spark_weather_checkpoint") \
    .outputMode("append") \
    .start()

query.awaitTermination()
