"""
🌬️ Spark Structured Streaming - Air Quality (Format JSON)
==========================================================
Projet PFE : Système Intelligent de Surveillance de la Qualité de l'Air
Auteurs : El Hassani Abdelmoughit & Boulghalegh Youssef

Ce script lit les données JSON du capteur PMS5003 depuis Kafka Confluent Cloud,
applique les transformations (zones, AQI, enrichissement) et écrit les résultats.

Format JSON attendu du capteur:
{
  "date": "2026-01-24",
  "time": "20:44:25",
  "pm10_cf1": 9,      // PM1.0 
  "pm25_cf1": 10,     // PM2.5
  "pm100_cf1": 10,    // PM10
  "pm10_std": 9,
  "pm25_std": 10,
  "pm100_std": 10,
  "gr03um": 1257,     // particles > 0.3um
  "gr05um": 395,      // particles > 0.5um
  "gr10um": 53,       // particles > 1.0um
  "gr25um": 0,        // particles > 2.5um
  "gr50um": 0,        // particles > 5.0um
  "gr100um": 0        // particles > 10.0um
}
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, when, rand, from_json,
    hour, dayofweek, month, concat_ws, to_timestamp,
    explode, array, struct, to_json,
    round as spark_round
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, BooleanType
)

# ============================================================
# CONFIGURATION KAFKA CONFLUENT CLOUD
# ============================================================
KAFKA_CONFIG = {
    "bootstrap_servers": "pkc-921jm.us-east-2.aws.confluent.cloud:9092",
    "security_protocol": "SASL_SSL",
    "sasl_mechanism": "PLAIN",
    "sasl_username": "ML4G7QKTNTKH5IBV",
    "sasl_password": "cfltAGGAi+lzTeltLpNCH3aF3sCG5/pbE0TemK59vWMBZicFAd8sicKkeVQCbh8g",
}

# Topics
INPUT_TOPIC = "topic_8"  # Topic avec données du capteur
OUTPUT_TOPIC = "air_quality_processed"

# Checkpoint
CHECKPOINT_DIR = "/checkpoint"

# ============================================================
# SCHÉMA JSON DU CAPTEUR PMS5003
# ============================================================
SENSOR_SCHEMA = StructType([
    StructField("date", StringType(), True),
    StructField("time", StringType(), True),
    StructField("pm10_cf1", IntegerType(), True),    # PM1.0
    StructField("pm25_cf1", IntegerType(), True),    # PM2.5
    StructField("pm100_cf1", IntegerType(), True),   # PM10
    StructField("pm10_std", IntegerType(), True),
    StructField("pm25_std", IntegerType(), True),
    StructField("pm100_std", IntegerType(), True),
    StructField("gr03um", IntegerType(), True),      # particles > 0.3µm
    StructField("gr05um", IntegerType(), True),      # particles > 0.5µm
    StructField("gr10um", IntegerType(), True),      # particles > 1.0µm
    StructField("gr25um", IntegerType(), True),      # particles > 2.5µm
    StructField("gr50um", IntegerType(), True),      # particles > 5.0µm
    StructField("gr100um", IntegerType(), True),     # particles > 10.0µm
])

# ============================================================
# CONFIGURATION DES ZONES DE MONTBÉLIARD
# ============================================================
# Format: (zone_id, zone_name, latitude, longitude, pm_factor, variability, rush_hour_boost, night_reduction)
ZONES_CONFIG = [
    ("centre_ville", "Centre-Ville", 47.5103, 6.7983, 1.2, 0.15, 1.3, 0.85),
    ("zone_industrielle", "Zone Industrielle", 47.4950, 6.8150, 1.4, 0.25, 1.1, 0.7),
    ("residentiel", "Quartier Résidentiel", 47.5150, 6.7850, 0.9, 0.10, 1.15, 0.95),
    ("parc_pres_la_rose", "Parc Près-la-Rose", 47.5050, 6.7880, 0.6, 0.08, 1.05, 1.0),
    ("peripherie", "Périphérie", 47.5200, 6.8100, 0.75, 0.12, 1.2, 0.9),
]

# ============================================================
# FONCTIONS DE CALCUL AQI (EPA)
# ============================================================
def calculate_aqi_value(pm25):
    """Calcule l'AQI basé sur PM2.5 (formule EPA)"""
    if pm25 is None or pm25 < 0:
        return 0
    
    breakpoints = [
        (0.0, 12.0, 0, 50),
        (12.1, 35.4, 51, 100),
        (35.5, 55.4, 101, 150),
        (55.5, 150.4, 151, 200),
        (150.5, 250.4, 201, 300),
        (250.5, 500.4, 301, 500),
    ]
    
    for c_low, c_high, aqi_low, aqi_high in breakpoints:
        if c_low <= pm25 <= c_high:
            aqi = ((aqi_high - aqi_low) / (c_high - c_low)) * (pm25 - c_low) + aqi_low
            return int(round(aqi))
    
    return 500 if pm25 > 500.4 else 0

# ============================================================
# CRÉATION DE LA SESSION SPARK
# ============================================================
def create_spark_session():
    return SparkSession.builder \
        .appName("AirQuality_JSON_Streaming") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.streaming.backpressure.enabled", "true") \
        .getOrCreate()

# ============================================================
# LECTURE KAFKA
# ============================================================
def read_from_kafka(spark):
    """Lit les données depuis Kafka Confluent Cloud"""
    
    jaas_config = (
        f'org.apache.kafka.common.security.plain.PlainLoginModule required '
        f'username="{KAFKA_CONFIG["sasl_username"]}" '
        f'password="{KAFKA_CONFIG["sasl_password"]}";'
    )
    
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_CONFIG["bootstrap_servers"]) \
        .option("kafka.security.protocol", KAFKA_CONFIG["security_protocol"]) \
        .option("kafka.sasl.mechanism", KAFKA_CONFIG["sasl_mechanism"]) \
        .option("kafka.sasl.jaas.config", jaas_config) \
        .option("subscribe", INPUT_TOPIC) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

# ============================================================
# PIPELINE DE TRAITEMENT
# ============================================================
def process_stream(kafka_df):
    """Applique toutes les transformations aux données du capteur"""
    
    # 1. Parser le JSON
    parsed_df = kafka_df \
        .selectExpr("CAST(value AS STRING) as json_str") \
        .select(from_json(col("json_str"), SENSOR_SCHEMA).alias("data")) \
        .select("data.*")
    
    # 2. Renommer les colonnes pour correspondre au format attendu
    renamed_df = parsed_df \
        .withColumn("pm1_0_atm_raw", col("pm10_cf1").cast("double")) \
        .withColumn("pm2_5_atm_raw", col("pm25_cf1").cast("double")) \
        .withColumn("pm10_atm_raw", col("pm100_cf1").cast("double")) \
        .withColumn("particles_03_raw", col("gr03um")) \
        .withColumn("particles_05_raw", col("gr05um")) \
        .withColumn("particles_10_raw", col("gr10um")) \
        .withColumn("particles_25_raw", col("gr25um")) \
        .withColumn("particles_50_raw", col("gr50um")) \
        .withColumn("particles_100_raw", col("gr100um"))
    
    # 3. Créer datetime et extraire l'heure
    with_datetime = renamed_df \
        .withColumn("datetime", to_timestamp(concat_ws(" ", col("date"), col("time")))) \
        .withColumn("hour", hour(col("datetime")))
    
    # 4. Créer les zones avec explode
    zones_array = array([
        struct(
            lit(z[0]).alias("zone_id"),
            lit(z[1]).alias("zone_name"),
            lit(z[2]).alias("latitude"),
            lit(z[3]).alias("longitude"),
            lit(z[4]).alias("pm_factor"),
            lit(z[5]).alias("variability"),
            lit(z[6]).alias("rush_hour_boost"),
            lit(z[7]).alias("night_reduction"),
        ) for z in ZONES_CONFIG
    ])
    
    exploded_df = with_datetime.withColumn("zone", explode(zones_array))
    
    # 5. Extraire les colonnes de zone
    zone_df = exploded_df.select(
        col("datetime"), col("date"), col("time"), col("hour"),
        col("pm1_0_atm_raw"), col("pm2_5_atm_raw"), col("pm10_atm_raw"),
        col("particles_03_raw"), col("particles_05_raw"), col("particles_10_raw"),
        col("particles_25_raw"), col("particles_50_raw"), col("particles_100_raw"),
        col("zone.zone_id").alias("zone_id"),
        col("zone.zone_name").alias("zone_name"),
        col("zone.latitude").alias("latitude"),
        col("zone.longitude").alias("longitude"),
        col("zone.pm_factor").alias("pm_factor"),
        col("zone.variability").alias("variability"),
        col("zone.rush_hour_boost").alias("rush_hour_boost"),
        col("zone.night_reduction").alias("night_reduction"),
    )
    
    # 6. Calculer les facteurs d'ajustement temporels
    adjusted_df = zone_df \
        .withColumn("is_rush_hour",
            when((col("hour") >= 7) & (col("hour") <= 9), True)
            .when((col("hour") >= 17) & (col("hour") <= 19), True)
            .otherwise(False)
        ) \
        .withColumn("is_night",
            when((col("hour") >= 22) | (col("hour") <= 5), True)
            .otherwise(False)
        )
    
    # 7. Calculer le facteur total
    adjusted_df = adjusted_df.withColumn(
        "total_factor",
        col("pm_factor") * 
        when(col("is_rush_hour"), col("rush_hour_boost")).otherwise(lit(1.0)) *
        when(col("is_night"), col("night_reduction")).otherwise(lit(1.0)) *
        (lit(1.0) + (rand() * 2 - 1) * col("variability"))
    )
    
    # 8. Appliquer les facteurs aux mesures PM
    augmented_df = adjusted_df \
        .withColumn("pm1_0_atm", spark_round(col("pm1_0_atm_raw") * col("total_factor"), 1)) \
        .withColumn("pm2_5_atm", spark_round(col("pm2_5_atm_raw") * col("total_factor"), 1)) \
        .withColumn("pm10_atm", spark_round(col("pm10_atm_raw") * col("total_factor"), 1))
    
    # Appliquer aux particules
    particle_factor = col("pm_factor") * when(col("is_rush_hour"), col("rush_hour_boost")).otherwise(lit(1.0))
    for pcol in ['particles_03', 'particles_05', 'particles_10', 'particles_25', 'particles_50', 'particles_100']:
        augmented_df = augmented_df.withColumn(
            pcol, 
            (col(f"{pcol}_raw") * particle_factor * (lit(1.0) + (rand() * 2 - 1) * col("variability"))).cast("int")
        )
    
    # 9. Calculer l'AQI avec SQL expressions (sans UDF pour meilleure performance)
    metrics_df = augmented_df.withColumn(
        "aqi",
        when(col("pm2_5_atm") <= 12.0, 
             spark_round((50.0 / 12.0) * col("pm2_5_atm"), 0))
        .when(col("pm2_5_atm") <= 35.4, 
             spark_round(((100.0 - 51.0) / (35.4 - 12.1)) * (col("pm2_5_atm") - 12.1) + 51.0, 0))
        .when(col("pm2_5_atm") <= 55.4, 
             spark_round(((150.0 - 101.0) / (55.4 - 35.5)) * (col("pm2_5_atm") - 35.5) + 101.0, 0))
        .when(col("pm2_5_atm") <= 150.4, 
             spark_round(((200.0 - 151.0) / (150.4 - 55.5)) * (col("pm2_5_atm") - 55.5) + 151.0, 0))
        .when(col("pm2_5_atm") <= 250.4, 
             spark_round(((300.0 - 201.0) / (250.4 - 150.5)) * (col("pm2_5_atm") - 150.5) + 201.0, 0))
        .when(col("pm2_5_atm") <= 500.4, 
             spark_round(((500.0 - 301.0) / (500.4 - 250.5)) * (col("pm2_5_atm") - 250.5) + 301.0, 0))
        .otherwise(500)
        .cast("int")
    )
    
    # 10. Ajouter catégorie AQI
    metrics_df = metrics_df.withColumn(
        "aqi_category",
        when(col("aqi") <= 50, "Bon")
        .when(col("aqi") <= 100, "Modéré")
        .when(col("aqi") <= 150, "Mauvais pour sensibles")
        .when(col("aqi") <= 200, "Mauvais")
        .when(col("aqi") <= 300, "Très mauvais")
        .otherwise("Dangereux")
    )
    
    # 11. Ajouter couleur AQI
    metrics_df = metrics_df.withColumn(
        "aqi_color",
        when(col("aqi") <= 50, "#00E400")
        .when(col("aqi") <= 100, "#FFFF00")
        .when(col("aqi") <= 150, "#FF7E00")
        .when(col("aqi") <= 200, "#FF0000")
        .when(col("aqi") <= 300, "#8F3F97")
        .otherwise("#7E0023")
    )
    
    # 12. Ajouter recommandation
    metrics_df = metrics_df.withColumn(
        "aqi_recommendation",
        when(col("aqi") <= 50, "Qualité satisfaisante")
        .when(col("aqi") <= 100, "Acceptable")
        .when(col("aqi") <= 150, "Sensibles : limiter efforts")
        .when(col("aqi") <= 200, "Éviter efforts prolongés")
        .when(col("aqi") <= 300, "Rester à l'intérieur")
        .otherwise("Urgence sanitaire")
    )
    
    # 13. Calculer pourcentage qualité d'air
    metrics_df = metrics_df.withColumn(
        "air_quality_percent",
        when(col("aqi") <= 0, 100.0)
        .when(col("aqi") >= 300, 0.0)
        .otherwise(spark_round(100.0 * (1.0 - (col("aqi").cast("double") / 300.0) ** 0.7), 1))
    )
    
    # 14. Enrichissement temporel
    # Correction: Spark dayofweek() retourne 1=Dimanche...7=Samedi
    enriched_df = metrics_df \
        .withColumn("day_of_week", ((dayofweek(col("datetime")) + 5) % 7).cast("int")) \
        .withColumn("day_name",
            when(col("day_of_week") == 0, "Monday")
            .when(col("day_of_week") == 1, "Tuesday")
            .when(col("day_of_week") == 2, "Wednesday")
            .when(col("day_of_week") == 3, "Thursday")
            .when(col("day_of_week") == 4, "Friday")
            .when(col("day_of_week") == 5, "Saturday")
            .otherwise("Sunday")
        ) \
        .withColumn("month", month(col("datetime"))) \
        .withColumn("is_weekend", when(col("day_of_week").isin([5, 6]), 1).otherwise(0)) \
        .withColumn("period",
            when((col("hour") >= 6) & (col("hour") < 12), "Matin")
            .when((col("hour") >= 12) & (col("hour") < 14), "Midi")
            .when((col("hour") >= 14) & (col("hour") < 18), "Après-midi")
            .when((col("hour") >= 18) & (col("hour") < 22), "Soir")
            .otherwise("Nuit")
        ) \
        .withColumn("sport_ok", when(col("aqi") <= 100, True).otherwise(False))
    
    # 15. Sélection finale des colonnes
    final_df = enriched_df.select(
        "datetime", "zone_id", "zone_name", "latitude", "longitude",
        "pm1_0_atm", "pm2_5_atm", "pm10_atm",
        "particles_03", "particles_05", "particles_10",
        "particles_25", "particles_50", "particles_100",
        "aqi", "aqi_category", "aqi_color", "aqi_recommendation",
        "air_quality_percent", "hour", "date", "time",
        "day_of_week", "day_name", "month", "is_weekend", "period", "sport_ok"
    )
    
    return final_df

# ============================================================
# ÉCRITURE DES RÉSULTATS
# ============================================================
def write_to_console(df):
    """Écrit vers la console (debug)"""
    return df.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .option("numRows", 10) \
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/console") \
        .start()

def write_to_kafka(df):
    """Écrit vers Kafka topic de sortie"""
    json_df = df.select(to_json(struct("*")).alias("value"))
    
    jaas_config = (
        f'org.apache.kafka.common.security.plain.PlainLoginModule required '
        f'username="{KAFKA_CONFIG["sasl_username"]}" '
        f'password="{KAFKA_CONFIG["sasl_password"]}";'
    )
    
    return json_df.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_CONFIG["bootstrap_servers"]) \
        .option("kafka.security.protocol", KAFKA_CONFIG["security_protocol"]) \
        .option("kafka.sasl.mechanism", KAFKA_CONFIG["sasl_mechanism"]) \
        .option("kafka.sasl.jaas.config", jaas_config) \
        .option("topic", OUTPUT_TOPIC) \
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/kafka") \
        .outputMode("append") \
        .start()

# ============================================================
# MAIN
# ============================================================
def main():
    print("=" * 70)
    print("🌬️  SPARK STREAMING - AIR QUALITY (JSON FORMAT)")
    print("=" * 70)
    print(f"📡 Kafka Confluent Cloud")
    print(f"📥 Topic d'entrée  : {INPUT_TOPIC}")
    print(f"📤 Topic de sortie : {OUTPUT_TOPIC}")
    print(f"🗺️  Zones          : {len(ZONES_CONFIG)}")
    for z in ZONES_CONFIG:
        print(f"   • {z[1]} (facteur ×{z[4]})")
    print("=" * 70)
    
    # Créer session Spark
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    print("✅ Session Spark créée")
    
    # Lire depuis Kafka
    kafka_df = read_from_kafka(spark)
    print("✅ Connexion Kafka établie")
    
    # Traiter le stream
    processed_df = process_stream(kafka_df)
    print("✅ Pipeline de traitement configuré")
    
    # Écrire les résultats
    # Mode Console (debug) - décommentez UN des deux:
    # query = write_to_console(processed_df)
    
    # Mode Kafka (production):
    query = write_to_kafka(processed_df)
    
    print("\n🚀 Streaming démarré... (Ctrl+C pour arrêter)\n")
    
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\n⛔ Arrêt du streaming")
        query.stop()

if __name__ == "__main__":
    main()