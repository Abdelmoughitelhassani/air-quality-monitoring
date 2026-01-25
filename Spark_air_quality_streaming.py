"""
🌬️ Spark Structured Streaming - Air Quality Real-Time Processing
============================================================
Projet PFE : Système Intelligent de Surveillance de la Qualité de l'Air
Auteurs : El Hassani Abdelmoughit & Boulghalegh Youssef

Ce script lit les données brutes depuis Kafka, applique:
1. L'augmentation des données pour 5 zones
2. Les transformations (AQI, périodes, etc.)
3. Écrit les résultats dans Kafka ou console
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, split, from_json, to_json, struct, lit, when, rand, 
    hour, dayofweek, month, date_format, concat_ws, to_timestamp,
    udf, explode, array, current_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, 
    TimestampType, ArrayType, BooleanType
)
import random

# ============================================================
# 1. CONFIGURATION KAFKA
# ============================================================
KAFKA_BOOTSTRAP_SERVERS = "pkc-921jm.us-east-2.aws.confluent.cloud:9092"
KAFKA_SECURITY_PROTOCOL = "SASL_SSL"
KAFKA_SASL_MECHANISM = "PLAIN"
KAFKA_SASL_USERNAME = "ML4G7QKTNTKH5IBV"
KAFKA_SASL_PASSWORD = "cfltAGGAi+lzTeltLpNCH3aF3sCG5/pbE0TemK59vWMBZicFAd8sicKkeVQCbh8g"

INPUT_TOPIC = "topic_8"  # Topic d'entrée (données brutes du capteur)
OUTPUT_TOPIC = "air_quality_processed"  # Topic de sortie (données traitées)

# JAAS Config pour l'authentification
JAAS_CONFIG = f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{KAFKA_SASL_USERNAME}" password="{KAFKA_SASL_PASSWORD}";'

# ============================================================
# 2. DÉFINITION DES 5 ZONES DE MONTBÉLIARD
# ============================================================
ZONES = {
    'centre_ville': {
        'name': 'Centre-Ville',
        'latitude': 47.5103,
        'longitude': 6.7983,
        'pm_factor': 1.2,
        'variability': 0.15,
        'rush_hour_boost': 1.3,
        'night_reduction': 0.85,
    },
    'zone_industrielle': {
        'name': 'Zone Industrielle',
        'latitude': 47.4950,
        'longitude': 6.8150,
        'pm_factor': 1.4,
        'variability': 0.25,
        'rush_hour_boost': 1.1,
        'night_reduction': 0.7,
    },
    'residentiel': {
        'name': 'Quartier Résidentiel',
        'latitude': 47.5150,
        'longitude': 6.7850,
        'pm_factor': 0.9,
        'variability': 0.10,
        'rush_hour_boost': 1.15,
        'night_reduction': 0.95,
    },
    'parc_pres_la_rose': {
        'name': 'Parc Près-la-Rose',
        'latitude': 47.5050,
        'longitude': 6.7880,
        'pm_factor': 0.6,
        'variability': 0.08,
        'rush_hour_boost': 1.05,
        'night_reduction': 1.0,
    },
    'peripherie': {
        'name': 'Périphérie',
        'latitude': 47.5200,
        'longitude': 6.8100,
        'pm_factor': 0.75,
        'variability': 0.12,
        'rush_hour_boost': 1.2,
        'night_reduction': 0.9,
    }
}

# ============================================================
# 3. SCHÉMA DES DONNÉES BRUTES DU CAPTEUR
# ============================================================
# Format: 2025-11-07,18:21:46,17,31,36,17,30,36,2694,836,184,31,6,4
RAW_SCHEMA = StructType([
    StructField("date", StringType(), True),
    StructField("time", StringType(), True),
    StructField("pm1_0_std", IntegerType(), True),
    StructField("pm2_5_std", IntegerType(), True),
    StructField("pm10_std", IntegerType(), True),
    StructField("pm1_0_atm", IntegerType(), True),
    StructField("pm2_5_atm", IntegerType(), True),
    StructField("pm10_atm", IntegerType(), True),
    StructField("particles_03", IntegerType(), True),
    StructField("particles_05", IntegerType(), True),
    StructField("particles_10", IntegerType(), True),
    StructField("particles_25", IntegerType(), True),
    StructField("particles_50", IntegerType(), True),
    StructField("particles_100", IntegerType(), True),
])

# ============================================================
# 4. FONCTIONS UDF POUR LES TRANSFORMATIONS
# ============================================================

def calculate_aqi_pm25(pm25):
    """Calcule l'AQI basé sur PM2.5 (formule EPA)"""
    if pm25 is None:
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
            return int(round(((aqi_high - aqi_low) / (c_high - c_low)) * (pm25 - c_low) + aqi_low))
    return 500 if pm25 > 500.4 else 0

def get_aqi_category(aqi):
    """Retourne la catégorie AQI"""
    if aqi is None:
        return "Inconnu"
    if aqi <= 50:
        return "Bon"
    elif aqi <= 100:
        return "Modéré"
    elif aqi <= 150:
        return "Mauvais pour sensibles"
    elif aqi <= 200:
        return "Mauvais"
    elif aqi <= 300:
        return "Très mauvais"
    else:
        return "Dangereux"

def get_aqi_color(aqi):
    """Retourne la couleur AQI"""
    if aqi is None:
        return "#808080"
    if aqi <= 50:
        return "#00E400"
    elif aqi <= 100:
        return "#FFFF00"
    elif aqi <= 150:
        return "#FF7E00"
    elif aqi <= 200:
        return "#FF0000"
    elif aqi <= 300:
        return "#8F3F97"
    else:
        return "#7E0023"

def get_aqi_recommendation(aqi):
    """Retourne la recommandation AQI"""
    if aqi is None:
        return "Données indisponibles"
    if aqi <= 50:
        return "Qualité satisfaisante"
    elif aqi <= 100:
        return "Acceptable"
    elif aqi <= 150:
        return "Sensibles : limiter efforts"
    elif aqi <= 200:
        return "Éviter efforts prolongés"
    elif aqi <= 300:
        return "Rester à l'intérieur"
    else:
        return "Urgence sanitaire"

def calculate_quality_percent(aqi):
    """Convertit AQI en pourcentage de qualité (0-100%)"""
    if aqi is None or aqi <= 0:
        return 100.0
    elif aqi >= 300:
        return 0.0
    return round(100 * (1 - (aqi / 300) ** 0.7), 1)

def get_period(hour_val):
    """Détermine la période de la journée"""
    if hour_val is None:
        return "Inconnu"
    if 6 <= hour_val < 12:
        return "Matin"
    elif 12 <= hour_val < 14:
        return "Midi"
    elif 14 <= hour_val < 18:
        return "Après-midi"
    elif 18 <= hour_val < 22:
        return "Soir"
    else:
        return "Nuit"

def get_day_name(day_of_week):
    """Retourne le nom du jour"""
    days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    if day_of_week is None or day_of_week < 0 or day_of_week > 6:
        return "Inconnu"
    return days[day_of_week]

def is_sport_ok(aqi):
    """Détermine si le sport en extérieur est recommandé"""
    if aqi is None:
        return False
    return aqi <= 100

# Enregistrement des UDFs
calculate_aqi_udf = udf(calculate_aqi_pm25, IntegerType())
get_aqi_category_udf = udf(get_aqi_category, StringType())
get_aqi_color_udf = udf(get_aqi_color, StringType())
get_aqi_recommendation_udf = udf(get_aqi_recommendation, StringType())
calculate_quality_percent_udf = udf(calculate_quality_percent, DoubleType())
get_period_udf = udf(get_period, StringType())
get_day_name_udf = udf(get_day_name, StringType())
is_sport_ok_udf = udf(is_sport_ok, BooleanType())

# ============================================================
# 5. CRÉATION DE LA SESSION SPARK
# ============================================================
def create_spark_session():
    """Crée et configure la session Spark"""
    spark = SparkSession.builder \
        .appName("AirQuality_RealTime_Processing") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark

# ============================================================
# 6. LECTURE DEPUIS KAFKA
# ============================================================
def read_from_kafka(spark):
    """Lit les données streaming depuis Kafka"""
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("kafka.security.protocol", KAFKA_SECURITY_PROTOCOL) \
        .option("kafka.sasl.mechanism", KAFKA_SASL_MECHANISM) \
        .option("kafka.sasl.jaas.config", JAAS_CONFIG) \
        .option("subscribe", INPUT_TOPIC) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

# ============================================================
# 7. PARSING DES DONNÉES BRUTES
# ============================================================
def parse_raw_data(df):
    """Parse les données brutes CSV en colonnes structurées"""
    # Extraire la valeur du message Kafka
    parsed = df.selectExpr("CAST(value AS STRING) as raw_data", "timestamp as kafka_timestamp")
    
    # Splitter les données CSV
    split_col = split(col("raw_data"), ",")
    
    return parsed.select(
        col("kafka_timestamp"),
        split_col.getItem(0).alias("date"),
        split_col.getItem(1).alias("time"),
        split_col.getItem(2).cast("int").alias("pm1_0_std"),
        split_col.getItem(3).cast("int").alias("pm2_5_std"),
        split_col.getItem(4).cast("int").alias("pm10_std"),
        split_col.getItem(5).cast("int").alias("pm1_0_atm"),
        split_col.getItem(6).cast("int").alias("pm2_5_atm"),
        split_col.getItem(7).cast("int").alias("pm10_atm"),
        split_col.getItem(8).cast("int").alias("particles_03"),
        split_col.getItem(9).cast("int").alias("particles_05"),
        split_col.getItem(10).cast("int").alias("particles_10"),
        split_col.getItem(11).cast("int").alias("particles_25"),
        split_col.getItem(12).cast("int").alias("particles_50"),
        split_col.getItem(13).cast("int").alias("particles_100"),
    )

# ============================================================
# 8. AUGMENTATION DES DONNÉES POUR LES 5 ZONES
# ============================================================
def augment_for_zones(df):
    """
    Duplique chaque mesure pour les 5 zones avec facteurs d'augmentation.
    Utilise une approche basée sur explode pour créer les données multi-zones.
    """
    # Créer un DataFrame avec les configurations de zones
    zones_data = [(k, v['name'], v['latitude'], v['longitude'], 
                   v['pm_factor'], v['variability'], 
                   v['rush_hour_boost'], v['night_reduction']) 
                  for k, v in ZONES.items()]
    
    # Ajouter les zones comme colonnes via cross join
    # Note: Dans Spark Streaming, on utilise une approche différente
    
    # Créer les données pour chaque zone en utilisant des expressions
    result_dfs = []
    
    for zone_id, config in ZONES.items():
        zone_df = df.withColumn("zone_id", lit(zone_id)) \
                    .withColumn("zone_name", lit(config['name'])) \
                    .withColumn("latitude", lit(config['latitude'])) \
                    .withColumn("longitude", lit(config['longitude'])) \
                    .withColumn("pm_factor", lit(config['pm_factor'])) \
                    .withColumn("variability", lit(config['variability'])) \
                    .withColumn("rush_hour_boost", lit(config['rush_hour_boost'])) \
                    .withColumn("night_reduction", lit(config['night_reduction']))
        result_dfs.append(zone_df)
    
    # Union de tous les DataFrames de zones
    augmented_df = result_dfs[0]
    for zone_df in result_dfs[1:]:
        augmented_df = augmented_df.union(zone_df)
    
    return augmented_df

def apply_zone_factors(df):
    """
    Applique les facteurs de zone aux valeurs PM.
    Inclut l'ajustement pour heures de pointe et nuit.
    """
    # Créer datetime et extraire l'heure
    df = df.withColumn("datetime", to_timestamp(concat_ws(" ", col("date"), col("time"))))
    df = df.withColumn("hour", hour(col("datetime")))
    
    # Déterminer si c'est une heure de pointe (7-9h ou 17-19h)
    df = df.withColumn("is_rush_hour", 
        when((col("hour") >= 7) & (col("hour") <= 9), True)
        .when((col("hour") >= 17) & (col("hour") <= 19), True)
        .otherwise(False)
    )
    
    # Déterminer si c'est la nuit (22h-5h)
    df = df.withColumn("is_night",
        when((col("hour") >= 22) | (col("hour") <= 5), True)
        .otherwise(False)
    )
    
    # Calculer le facteur total
    df = df.withColumn("total_factor",
        col("pm_factor") * 
        when(col("is_rush_hour"), col("rush_hour_boost")).otherwise(1.0) *
        when(col("is_night"), col("night_reduction")).otherwise(1.0) *
        (1 + (rand() * 2 - 1) * col("variability"))  # Variabilité aléatoire
    )
    
    # Appliquer les facteurs aux colonnes PM
    pm_columns = ['pm1_0_std', 'pm2_5_std', 'pm10_std', 'pm1_0_atm', 'pm2_5_atm', 'pm10_atm']
    for col_name in pm_columns:
        df = df.withColumn(col_name, (col(col_name) * col("total_factor")).cast("double"))
    
    # Appliquer aux particules (avec facteur sans réduction nuit)
    particle_factor = col("pm_factor") * when(col("is_rush_hour"), col("rush_hour_boost")).otherwise(1.0)
    particle_columns = ['particles_03', 'particles_05', 'particles_10', 
                        'particles_25', 'particles_50', 'particles_100']
    for col_name in particle_columns:
        df = df.withColumn(col_name, 
            (col(col_name) * particle_factor * (1 + (rand() * 2 - 1) * col("variability"))).cast("int")
        )
    
    return df

# ============================================================
# 9. CALCUL DES MÉTRIQUES AQI ET ENRICHISSEMENT
# ============================================================
def calculate_metrics(df):
    """Calcule l'AQI et enrichit avec les métadonnées temporelles"""
    
    # Calcul de l'AQI basé sur PM2.5
    df = df.withColumn("aqi", calculate_aqi_udf(col("pm2_5_atm")))
    
    # Catégorie, couleur et recommandation AQI
    df = df.withColumn("aqi_category", get_aqi_category_udf(col("aqi")))
    df = df.withColumn("aqi_color", get_aqi_color_udf(col("aqi")))
    df = df.withColumn("aqi_recommendation", get_aqi_recommendation_udf(col("aqi")))
    
    # Pourcentage de qualité d'air
    df = df.withColumn("air_quality_percent", calculate_quality_percent_udf(col("aqi")))
    
    # Enrichissement temporel
    # Correction: Spark dayofweek() retourne 1=Dimanche...7=Samedi
    # Python weekday() retourne 0=Lundi...6=Dimanche
    df = df.withColumn("day_of_week", ((dayofweek(col("datetime")) + 5) % 7).cast("int"))
    df = df.withColumn("day_name", get_day_name_udf(col("day_of_week")))
    df = df.withColumn("month", month(col("datetime")))
    df = df.withColumn("is_weekend", when(col("day_of_week").isin([5, 6]), 1).otherwise(0))
    df = df.withColumn("period", get_period_udf(col("hour")))
    
    # Sport OK ?
    df = df.withColumn("sport_ok", is_sport_ok_udf(col("aqi")))
    
    return df

# ============================================================
# 10. SÉLECTION DES COLONNES FINALES
# ============================================================
def select_final_columns(df):
    """Sélectionne et ordonne les colonnes finales"""
    return df.select(
        col("datetime"),
        col("zone_id"),
        col("zone_name"),
        col("latitude"),
        col("longitude"),
        col("pm1_0_atm").cast("double").alias("pm1_0_atm"),
        col("pm2_5_atm").cast("double").alias("pm2_5_atm"),
        col("pm10_atm").cast("double").alias("pm10_atm"),
        col("particles_03"),
        col("particles_05"),
        col("particles_10"),
        col("particles_25"),
        col("particles_50"),
        col("particles_100"),
        col("aqi"),
        col("aqi_category"),
        col("aqi_color"),
        col("aqi_recommendation"),
        col("air_quality_percent"),
        col("hour"),
        col("date"),
        col("time"),
        col("day_of_week"),
        col("day_name"),
        col("month"),
        col("is_weekend"),
        col("period"),
        col("sport_ok")
    )

# ============================================================
# 11. ÉCRITURE VERS KAFKA
# ============================================================
def write_to_kafka(df):
    """Écrit les données traitées vers Kafka"""
    # Convertir en JSON pour Kafka
    json_df = df.select(to_json(struct("*")).alias("value"))
    
    return json_df.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("kafka.security.protocol", KAFKA_SECURITY_PROTOCOL) \
        .option("kafka.sasl.mechanism", KAFKA_SASL_MECHANISM) \
        .option("kafka.sasl.jaas.config", JAAS_CONFIG) \
        .option("topic", OUTPUT_TOPIC) \
        .option("checkpointLocation", "/tmp/checkpoint_kafka") \
        .outputMode("append") \
        .start()

def write_to_console(df):
    """Écrit les données traitées vers la console (pour debug)"""
    return df.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .option("numRows", 10) \
        .start()

# ============================================================
# 12. PIPELINE PRINCIPAL
# ============================================================
def process_air_quality_stream():
    """Pipeline principal de traitement en temps réel"""
    
    print("=" * 60)
    print("🌬️ Démarrage du traitement Spark Streaming")
    print("=" * 60)
    print(f"📥 Topic d'entrée: {INPUT_TOPIC}")
    print(f"📤 Topic de sortie: {OUTPUT_TOPIC}")
    print(f"🗺️ Zones configurées: {len(ZONES)}")
    for zone_id, config in ZONES.items():
        print(f"   • {config['name']} (facteur: ×{config['pm_factor']})")
    print("=" * 60)
    
    # Créer la session Spark
    spark = create_spark_session()
    print("✅ Session Spark créée")
    
    # Lire depuis Kafka
    kafka_df = read_from_kafka(spark)
    print("✅ Connexion Kafka établie")
    
    # Parser les données brutes
    parsed_df = parse_raw_data(kafka_df)
    print("✅ Parsing des données configuré")
    
    # Augmenter pour les 5 zones
    augmented_df = augment_for_zones(parsed_df)
    print("✅ Augmentation multi-zones configurée")
    
    # Appliquer les facteurs de zone
    factored_df = apply_zone_factors(augmented_df)
    print("✅ Facteurs de zone appliqués")
    
    # Calculer les métriques
    metrics_df = calculate_metrics(factored_df)
    print("✅ Métriques AQI calculées")
    
    # Sélectionner les colonnes finales
    final_df = select_final_columns(metrics_df)
    print("✅ Colonnes finales sélectionnées")
    
    # Écrire vers Kafka (ou console pour debug)
    # Pour tester, utilisez write_to_console(final_df)
    # Pour production, utilisez write_to_kafka(final_df)
    
    print("\n🚀 Démarrage du streaming...")
    print("   (Ctrl+C pour arrêter)\n")
    
    # Choix du mode de sortie
    # Mode Console (debug)
    query = write_to_console(final_df)
    
    # Mode Kafka (production) - décommentez si nécessaire
    # query = write_to_kafka(final_df)
    
    query.awaitTermination()

# ============================================================
# 13. POINT D'ENTRÉE
# ============================================================
if __name__ == "__main__":
    try:
        process_air_quality_stream()
    except KeyboardInterrupt:
        print("\n⛔ Arrêt du traitement demandé")
    except Exception as e:
        print(f"\n❌ Erreur: {e}")
        raise