"""
🌬️ Spark Structured Streaming - Air Quality Multi-Sensors Virtuels
====================================================================
Projet PFE : Système Intelligent de Surveillance de la Qualité de l'Air
Auteurs : El Hassani Abdelmoughit & Boulghalegh Youssef

ARCHITECTURE:
- 1 capteur physique (PMS5003) envoie les données brutes vers Kafka
- Spark crée N capteurs VIRTUELS par zone (simulation)
- Chaque capteur virtuel a ses propres variations
- Calcul AQI individuel par capteur virtuel
- Calcul moyenne AQI par zone

CONFIGURATION DYNAMIQUE:
- Ajouter/supprimer des capteurs virtuels dans SENSORS_CONFIG
- Le code s'adapte automatiquement
- Supporte des centaines/milliers de capteurs virtuels

FORMAT JSON D'ENTRÉE (capteur physique):
{
  "date": "2026-01-24",
  "time": "20:44:25",
  "pm10_cf1": 9,
  "pm25_cf1": 10,
  "pm100_cf1": 10,
  ...
}
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, when, rand, from_json, coalesce,
    hour, dayofweek, month, concat_ws, to_timestamp,
    avg, count, min, max, first, collect_list,
    window, current_timestamp, expr, struct, to_json,
    explode, array, monotonically_increasing_id,
    round as spark_round
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DoubleType, BooleanType, TimestampType
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
INPUT_TOPIC = "topic_9"  # Données brutes du capteur physique
OUTPUT_TOPIC_SENSORS = "air_quality_sensors"  # Données par capteur virtuel
OUTPUT_TOPIC_ZONES = "air_quality_zones"  # Moyennes par zone

# Checkpoint
CHECKPOINT_DIR = "/checkpoint"

# Fenêtre d'agrégation pour les moyennes par zone
AGGREGATION_WINDOW = "1 minute"
WATERMARK_DELAY = "30 seconds"

# ============================================================
# 🔧 CONFIGURATION DES CAPTEURS VIRTUELS PAR ZONE
# ============================================================
# MODIFIEZ ICI pour ajouter/supprimer des capteurs virtuels
# Chaque capteur a:
#   - id: identifiant unique
#   - lat/lon: coordonnées GPS
#   - variation: facteur de variation local (0.9 = -10%, 1.1 = +10%)
#   - description: emplacement du capteur

SENSORS_CONFIG = {
    "centre_ville": {
        "zone_name": "Centre-Ville",
        "pm_factor": 1.2,           # Facteur de pollution de la zone
        "variability": 0.15,         # Variabilité de la zone
        "rush_hour_boost": 1.3,
        "night_reduction": 0.85,
        "sensors": [
            {"id": "CV_001", "lat": 47.5103, "lon": 6.7983, "variation": 1.0, "desc": "Mairie"},
            {"id": "CV_002", "lat": 47.5110, "lon": 6.7970, "variation": 1.05, "desc": "Gare"},
            {"id": "CV_003", "lat": 47.5095, "lon": 6.7995, "variation": 0.95, "desc": "Place centrale"},
            # ➕ AJOUTEZ VOS CAPTEURS ICI:
            # {"id": "CV_004", "lat": 47.5100, "lon": 6.7980, "variation": 1.02, "desc": "École"},
        ]
    },
    "zone_industrielle": {
        "zone_name": "Zone Industrielle",
        "pm_factor": 1.4,
        "variability": 0.25,
        "rush_hour_boost": 1.1,
        "night_reduction": 0.7,
        "sensors": [
            {"id": "ZI_001", "lat": 47.4950, "lon": 6.8150, "variation": 1.0, "desc": "Entrée zone"},
            {"id": "ZI_002", "lat": 47.4940, "lon": 6.8170, "variation": 1.15, "desc": "Usine PSA"},
            {"id": "ZI_003", "lat": 47.4960, "lon": 6.8130, "variation": 0.90, "desc": "Entrepôts"},
            {"id": "ZI_004", "lat": 47.4945, "lon": 6.8160, "variation": 1.10, "desc": "Parking PL"},
            # ➕ AJOUTEZ VOS CAPTEURS ICI:
        ]
    },
    "residentiel": {
        "zone_name": "Quartier Résidentiel",
        "pm_factor": 0.9,
        "variability": 0.10,
        "rush_hour_boost": 1.15,
        "night_reduction": 0.95,
        "sensors": [
            {"id": "RES_001", "lat": 47.5150, "lon": 6.7850, "variation": 1.0, "desc": "École primaire"},
            {"id": "RES_002", "lat": 47.5160, "lon": 6.7840, "variation": 0.95, "desc": "Lotissement Nord"},
            # ➕ AJOUTEZ VOS CAPTEURS ICI:
        ]
    },
    "parc_pres_la_rose": {
        "zone_name": "Parc Près-la-Rose",
        "pm_factor": 0.6,
        "variability": 0.08,
        "rush_hour_boost": 1.05,
        "night_reduction": 1.0,
        "sensors": [
            {"id": "PARC_001", "lat": 47.5050, "lon": 6.7880, "variation": 1.0, "desc": "Entrée principale"},
            {"id": "PARC_002", "lat": 47.5055, "lon": 6.7890, "variation": 0.98, "desc": "Lac"},
            {"id": "PARC_003", "lat": 47.5045, "lon": 6.7875, "variation": 1.02, "desc": "Aire de jeux"},
            # ➕ AJOUTEZ VOS CAPTEURS ICI:
        ]
    },
    "peripherie": {
        "zone_name": "Périphérie",
        "pm_factor": 0.75,
        "variability": 0.12,
        "rush_hour_boost": 1.2,
        "night_reduction": 0.9,
        "sensors": [
            {"id": "PER_001", "lat": 47.5200, "lon": 6.8100, "variation": 1.0, "desc": "Route nationale"},
            {"id": "PER_002", "lat": 47.5210, "lon": 6.8090, "variation": 1.05, "desc": "Zone commerciale"},
            # ➕ AJOUTEZ VOS CAPTEURS ICI:
        ]
    },
}

# ============================================================
# SCHÉMA JSON DU CAPTEUR PHYSIQUE (format actuel)
# ============================================================
SENSOR_SCHEMA = StructType([
    StructField("date", StringType(), True),
    StructField("time", StringType(), True),
    StructField("pm10_cf1", IntegerType(), True),
    StructField("pm25_cf1", IntegerType(), True),
    StructField("pm100_cf1", IntegerType(), True),
    StructField("pm10_std", IntegerType(), True),
    StructField("pm25_std", IntegerType(), True),
    StructField("pm100_std", IntegerType(), True),
    StructField("gr03um", IntegerType(), True),
    StructField("gr05um", IntegerType(), True),
    StructField("gr10um", IntegerType(), True),
    StructField("gr25um", IntegerType(), True),
    StructField("gr50um", IntegerType(), True),
    StructField("gr100um", IntegerType(), True),
])

# ============================================================
# GÉNÉRATION DE LA LISTE DES CAPTEURS VIRTUELS
# ============================================================
def get_all_virtual_sensors():
    """
    Génère la liste complète des capteurs virtuels depuis SENSORS_CONFIG.
    Retourne une liste de tuples pour créer le array Spark.
    """
    sensors = []
    for zone_id, zone_config in SENSORS_CONFIG.items():
        for sensor in zone_config["sensors"]:
            sensors.append({
                "sensor_id": sensor["id"],
                "zone_id": zone_id,
                "zone_name": zone_config["zone_name"],
                "latitude": sensor["lat"],
                "longitude": sensor["lon"],
                "sensor_variation": sensor["variation"],
                "pm_factor": zone_config["pm_factor"],
                "variability": zone_config["variability"],
                "rush_hour_boost": zone_config["rush_hour_boost"],
                "night_reduction": zone_config["night_reduction"],
            })
    return sensors

def print_sensors_summary():
    """Affiche le résumé des capteurs virtuels configurés"""
    print("\n📊 CAPTEURS VIRTUELS CONFIGURÉS:")
    print("-" * 70)
    total = 0
    for zone_id, zone_config in SENSORS_CONFIG.items():
        n = len(zone_config["sensors"])
        total += n
        print(f"   {zone_config['zone_name']:25} ({zone_id})")
        print(f"   └─ Facteur pollution: ×{zone_config['pm_factor']}")
        print(f"   └─ Capteurs: {n}")
        for s in zone_config["sensors"]:
            print(f"      • {s['id']:12} | ({s['lat']:.4f}, {s['lon']:.4f}) | var={s['variation']} | {s['desc']}")
        print()
    print("-" * 70)
    print(f"   🔢 TOTAL: {total} capteurs virtuels dans {len(SENSORS_CONFIG)} zones")
    print()

# ============================================================
# CRÉATION DE LA SESSION SPARK
# ============================================================
def create_spark_session():
    return SparkSession.builder \
        .appName("AirQuality_VirtualSensors_Streaming") \
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
# PIPELINE DE TRAITEMENT - CAPTEURS VIRTUELS
# ============================================================
def process_with_virtual_sensors(kafka_df):
    """
    Traite les données du capteur physique et crée les capteurs virtuels.
    1 message entrant → N messages sortants (un par capteur virtuel)
    """
    
    # 1. Parser le JSON du capteur physique
    parsed_df = kafka_df \
        .selectExpr("CAST(value AS STRING) as json_str", "timestamp as kafka_timestamp") \
        .select(
            from_json(col("json_str"), SENSOR_SCHEMA).alias("data"),
            col("kafka_timestamp")
        ) \
        .select("data.*", "kafka_timestamp")
    
    # 2. Créer datetime et extraire l'heure
    with_datetime = parsed_df \
        .withColumn("datetime", to_timestamp(concat_ws(" ", col("date"), col("time")))) \
        .withColumn("hour", hour(col("datetime"))) \
        .withColumn("pm1_raw", col("pm10_cf1").cast("double")) \
        .withColumn("pm25_raw", col("pm25_cf1").cast("double")) \
        .withColumn("pm10_raw", col("pm100_cf1").cast("double"))
    
    # 3. Créer l'array des capteurs virtuels depuis la config
    virtual_sensors = get_all_virtual_sensors()
    
    sensors_array = array([
        struct(
            lit(s["sensor_id"]).alias("sensor_id"),
            lit(s["zone_id"]).alias("zone_id"),
            lit(s["zone_name"]).alias("zone_name"),
            lit(s["latitude"]).alias("latitude"),
            lit(s["longitude"]).alias("longitude"),
            lit(s["sensor_variation"]).alias("sensor_variation"),
            lit(s["pm_factor"]).alias("pm_factor"),
            lit(s["variability"]).alias("variability"),
            lit(s["rush_hour_boost"]).alias("rush_hour_boost"),
            lit(s["night_reduction"]).alias("night_reduction"),
        ) for s in virtual_sensors
    ])
    
    # 4. Exploser pour créer 1 ligne par capteur virtuel
    exploded_df = with_datetime.withColumn("sensor", explode(sensors_array))
    
    # 5. Extraire les colonnes du capteur virtuel
    sensor_df = exploded_df.select(
        # Données temporelles
        col("datetime"), col("date"), col("time"), col("hour"), col("kafka_timestamp"),
        # Données brutes du capteur physique
        col("pm1_raw"), col("pm25_raw"), col("pm10_raw"),
        col("gr03um"), col("gr05um"), col("gr10um"),
        col("gr25um"), col("gr50um"), col("gr100um"),
        # Configuration du capteur virtuel
        col("sensor.sensor_id").alias("sensor_id"),
        col("sensor.zone_id").alias("zone_id"),
        col("sensor.zone_name").alias("zone_name"),
        col("sensor.latitude").alias("latitude"),
        col("sensor.longitude").alias("longitude"),
        col("sensor.sensor_variation").alias("sensor_variation"),
        col("sensor.pm_factor").alias("pm_factor"),
        col("sensor.variability").alias("variability"),
        col("sensor.rush_hour_boost").alias("rush_hour_boost"),
        col("sensor.night_reduction").alias("night_reduction"),
    )
    
    # 6. Calculer les facteurs temporels
    adjusted_df = sensor_df \
        .withColumn("is_rush_hour",
            when((col("hour") >= 7) & (col("hour") <= 9), True)
            .when((col("hour") >= 17) & (col("hour") <= 19), True)
            .otherwise(False)
        ) \
        .withColumn("is_night",
            when((col("hour") >= 22) | (col("hour") <= 5), True)
            .otherwise(False)
        )
    
    # 7. Calculer le facteur total (zone + capteur + temps + aléatoire)
    adjusted_df = adjusted_df.withColumn(
        "total_factor",
        col("pm_factor") *                                                    # Facteur de la zone
        col("sensor_variation") *                                             # Variation du capteur
        when(col("is_rush_hour"), col("rush_hour_boost")).otherwise(lit(1.0)) *  # Heures de pointe
        when(col("is_night"), col("night_reduction")).otherwise(lit(1.0)) *      # Nuit
        (lit(1.0) + (rand() * 2 - 1) * col("variability"))                    # Variation aléatoire
    )
    
    # 8. Appliquer les facteurs aux mesures PM
    augmented_df = adjusted_df \
        .withColumn("pm1_0_atm", spark_round(col("pm1_raw") * col("total_factor"), 1)) \
        .withColumn("pm2_5_atm", spark_round(col("pm25_raw") * col("total_factor"), 1)) \
        .withColumn("pm10_atm", spark_round(col("pm10_raw") * col("total_factor"), 1))
    
    # 9. Appliquer aux particules
    particle_factor = col("pm_factor") * col("sensor_variation") * \
        when(col("is_rush_hour"), col("rush_hour_boost")).otherwise(lit(1.0))
    
    augmented_df = augmented_df \
        .withColumn("particles_03", (col("gr03um") * particle_factor * (lit(1.0) + (rand() * 2 - 1) * col("variability"))).cast("int")) \
        .withColumn("particles_05", (col("gr05um") * particle_factor * (lit(1.0) + (rand() * 2 - 1) * col("variability"))).cast("int")) \
        .withColumn("particles_10", (col("gr10um") * particle_factor * (lit(1.0) + (rand() * 2 - 1) * col("variability"))).cast("int")) \
        .withColumn("particles_25", (col("gr25um") * particle_factor * (lit(1.0) + (rand() * 2 - 1) * col("variability"))).cast("int")) \
        .withColumn("particles_50", (col("gr50um") * particle_factor * (lit(1.0) + (rand() * 2 - 1) * col("variability"))).cast("int")) \
        .withColumn("particles_100", (col("gr100um") * particle_factor * (lit(1.0) + (rand() * 2 - 1) * col("variability"))).cast("int"))
    
    # 10. Calculer l'AQI
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
    
    # 11. Ajouter catégorie, couleur, recommandation
    metrics_df = metrics_df \
        .withColumn("aqi_category",
            when(col("aqi") <= 50, "Bon")
            .when(col("aqi") <= 100, "Modéré")
            .when(col("aqi") <= 150, "Mauvais pour sensibles")
            .when(col("aqi") <= 200, "Mauvais")
            .when(col("aqi") <= 300, "Très mauvais")
            .otherwise("Dangereux")
        ) \
        .withColumn("aqi_color",
            when(col("aqi") <= 50, "#00E400")
            .when(col("aqi") <= 100, "#FFFF00")
            .when(col("aqi") <= 150, "#FF7E00")
            .when(col("aqi") <= 200, "#FF0000")
            .when(col("aqi") <= 300, "#8F3F97")
            .otherwise("#7E0023")
        ) \
        .withColumn("aqi_recommendation",
            when(col("aqi") <= 50, "Qualité satisfaisante")
            .when(col("aqi") <= 100, "Acceptable")
            .when(col("aqi") <= 150, "Sensibles : limiter efforts")
            .when(col("aqi") <= 200, "Éviter efforts prolongés")
            .when(col("aqi") <= 300, "Rester à l'intérieur")
            .otherwise("Urgence sanitaire")
        ) \
        .withColumn("air_quality_percent",
            when(col("aqi") <= 0, 100.0)
            .when(col("aqi") >= 300, 0.0)
            .otherwise(spark_round(100.0 * (1.0 - (col("aqi").cast("double") / 300.0) ** 0.7), 1))
        )
    
    # 12. Enrichissement temporel
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
    
    # 13. Sélection finale - données par capteur virtuel
    final_sensor_df = enriched_df.select(
        # Identifiants
        "sensor_id", "zone_id", "zone_name", "latitude", "longitude",
        # Timestamp
        "datetime", "date", "time", "hour",
        # Mesures PM
        "pm1_0_atm", "pm2_5_atm", "pm10_atm",
        # Particules
        "particles_03", "particles_05", "particles_10",
        "particles_25", "particles_50", "particles_100",
        # AQI
        "aqi", "aqi_category", "aqi_color", "aqi_recommendation",
        "air_quality_percent",
        # Temporel
        "day_of_week", "day_name", "month", "is_weekend", "period",
        # Sport
        "sport_ok"
    )
    
    return final_sensor_df

# ============================================================
# AGRÉGATION PAR ZONE (Moyennes)
# ============================================================
def aggregate_by_zone(sensor_df):
    """Calcule les moyennes par zone avec fenêtre temporelle"""
    
    # Ajouter watermark pour le streaming
    windowed_df = sensor_df \
        .withWatermark("datetime", WATERMARK_DELAY)
    
    # Agrégation par zone et fenêtre temporelle
    zone_aggregated = windowed_df \
        .groupBy(
            window(col("datetime"), AGGREGATION_WINDOW),
            col("zone_id"),
            col("zone_name")
        ) \
        .agg(
            # Nombre de capteurs actifs dans la zone
            count("sensor_id").alias("active_sensors"),
            
            # Moyennes PM
            spark_round(avg("pm1_0_atm"), 1).alias("avg_pm1_0"),
            spark_round(avg("pm2_5_atm"), 1).alias("avg_pm2_5"),
            spark_round(avg("pm10_atm"), 1).alias("avg_pm10"),
            
            # Min/Max PM2.5
            spark_round(min("pm2_5_atm"), 1).alias("min_pm2_5"),
            spark_round(max("pm2_5_atm"), 1).alias("max_pm2_5"),
            
            # Moyennes AQI
            spark_round(avg("aqi"), 0).alias("avg_aqi"),
            min("aqi").alias("min_aqi"),
            max("aqi").alias("max_aqi"),
            
            # Moyenne qualité air
            spark_round(avg("air_quality_percent"), 1).alias("avg_air_quality_percent"),
            
            # Coordonnées moyennes de la zone (centre)
            spark_round(avg("latitude"), 4).alias("zone_latitude"),
            spark_round(avg("longitude"), 4).alias("zone_longitude"),
            
            # Liste des capteurs actifs
            collect_list("sensor_id").alias("sensor_ids"),
        )
    
    # Ajouter les métadonnées AQI pour la zone
    zone_with_category = zone_aggregated \
        .withColumn("zone_aqi_category",
            when(col("avg_aqi") <= 50, "Bon")
            .when(col("avg_aqi") <= 100, "Modéré")
            .when(col("avg_aqi") <= 150, "Mauvais pour sensibles")
            .when(col("avg_aqi") <= 200, "Mauvais")
            .when(col("avg_aqi") <= 300, "Très mauvais")
            .otherwise("Dangereux")
        ) \
        .withColumn("zone_aqi_color",
            when(col("avg_aqi") <= 50, "#00E400")
            .when(col("avg_aqi") <= 100, "#FFFF00")
            .when(col("avg_aqi") <= 150, "#FF7E00")
            .when(col("avg_aqi") <= 200, "#FF0000")
            .when(col("avg_aqi") <= 300, "#8F3F97")
            .otherwise("#7E0023")
        ) \
        .withColumn("zone_recommendation",
            when(col("avg_aqi") <= 50, "Qualité satisfaisante")
            .when(col("avg_aqi") <= 100, "Acceptable")
            .when(col("avg_aqi") <= 150, "Sensibles : limiter efforts")
            .when(col("avg_aqi") <= 200, "Éviter efforts prolongés")
            .when(col("avg_aqi") <= 300, "Rester à l'intérieur")
            .otherwise("Urgence sanitaire")
        )
    
    # Format final pour les zones
    zone_df = zone_with_category.select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "zone_id", "zone_name",
        "active_sensors",
        "zone_latitude", "zone_longitude",
        "avg_pm1_0", "avg_pm2_5", "avg_pm10",
        "min_pm2_5", "max_pm2_5",
        "avg_aqi", "min_aqi", "max_aqi",
        "zone_aqi_category", "zone_aqi_color", "zone_recommendation",
        "avg_air_quality_percent",
        "sensor_ids"
    )
    
    return zone_df

# ============================================================
# ÉCRITURE DES RÉSULTATS
# ============================================================
def write_to_console_sensors(df, query_name):
    """Écrit les données capteurs vers la console"""
    return df.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .option("numRows", 20) \
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/{query_name}") \
        .queryName(query_name) \
        .start()

def write_to_console_zones(df, query_name):
    """Écrit les données zones vers la console"""
    return df.writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", False) \
        .option("numRows", 10) \
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/{query_name}") \
        .queryName(query_name) \
        .start()

def write_to_kafka_sensors(df):
    """Écrit les données capteurs vers Kafka"""
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
        .option("topic", OUTPUT_TOPIC_SENSORS) \
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/kafka_sensors") \
        .outputMode("append") \
        .start()

def write_to_kafka_zones(df):
    """Écrit les moyennes zones vers Kafka"""
    # Convertir sensor_ids array en string pour JSON
    json_df = df \
        .withColumn("sensor_ids_str", concat_ws(",", col("sensor_ids"))) \
        .drop("sensor_ids") \
        .select(to_json(struct("*")).alias("value"))
    
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
        .option("topic", OUTPUT_TOPIC_ZONES) \
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/kafka_zones") \
        .outputMode("update") \
        .start()

# ============================================================
# MAIN
# ============================================================
def main():
    print("=" * 80)
    print("🌬️  SPARK STREAMING - CAPTEURS VIRTUELS")
    print("=" * 80)
    print(f"📡 Kafka Confluent Cloud")
    print(f"📥 Topic d'entrée (capteur physique) : {INPUT_TOPIC}")
    print(f"📤 Topic capteurs virtuels           : {OUTPUT_TOPIC_SENSORS}")
    print(f"📤 Topic moyennes zones              : {OUTPUT_TOPIC_ZONES}")
    print(f"⏱️  Fenêtre d'agrégation              : {AGGREGATION_WINDOW}")
    
    # Afficher la configuration des capteurs
    print_sensors_summary()
    
    # Compter le total
    total_sensors = sum(len(z["sensors"]) for z in SENSORS_CONFIG.values())
    print(f"📊 1 message entrant → {total_sensors} messages sortants (capteurs virtuels)")
    print("=" * 80)
    
    # Créer session Spark
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    print("\n✅ Session Spark créée")
    
    # Lire depuis Kafka
    kafka_df = read_from_kafka(spark)
    print("✅ Connexion Kafka établie")
    
    # Traiter avec capteurs virtuels
    sensor_df = process_with_virtual_sensors(kafka_df)
    print("✅ Pipeline capteurs virtuels configuré")
    
    # Agréger par zone
    zone_df = aggregate_by_zone(sensor_df)
    print("✅ Pipeline zones (moyennes) configuré")
    
    # ============================================
    # MODE DE SORTIE - Choisir UN des modes:
    # ============================================
    
    # MODE 1: Console (Debug) - Affiche les deux flux
    print("\n📺 Mode: Console (Debug)")
    query_sensors = write_to_console_sensors(sensor_df, "sensors_stream")
    query_zones = write_to_console_zones(zone_df, "zones_stream")
    
    # MODE 2: Kafka (Production) - Décommenter pour activer
    # print("\n📤 Mode: Kafka (Production)")
    # query_sensors = write_to_kafka_sensors(sensor_df)
    # query_zones = write_to_kafka_zones(zone_df)
    
    print("\n🚀 Streaming démarré... (Ctrl+C pour arrêter)")
    print(f"   → Flux 1: {total_sensors} capteurs virtuels par message")
    print(f"   → Flux 2: Moyennes par zone (agrégées par {AGGREGATION_WINDOW})\n")
    
    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("\n⛔ Arrêt du streaming")
        query_sensors.stop()
        query_zones.stop()

if __name__ == "__main__":
    main()