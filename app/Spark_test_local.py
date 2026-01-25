"""
🧪 Test Local - Air Quality Transformations
============================================
Ce script permet de tester les transformations sans Kafka,
en utilisant un fichier CSV local ou des données simulées.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, when, rand, hour, dayofweek, month, 
    concat_ws, to_timestamp, explode, array, struct,
    round as spark_round
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType
)
from pyspark.sql.functions import udf

# ============================================================
# CONFIGURATION DES ZONES
# ============================================================
ZONES_CONFIG = [
    ("centre_ville", "Centre-Ville", 47.5103, 6.7983, 1.2, 0.15, 1.3, 0.85),
    ("zone_industrielle", "Zone Industrielle", 47.4950, 6.8150, 1.4, 0.25, 1.1, 0.7),
    ("residentiel", "Quartier Résidentiel", 47.5150, 6.7850, 0.9, 0.10, 1.15, 0.95),
    ("parc_pres_la_rose", "Parc Près-la-Rose", 47.5050, 6.7880, 0.6, 0.08, 1.05, 1.0),
    ("peripherie", "Périphérie", 47.5200, 6.8100, 0.75, 0.12, 1.2, 0.9),
]

# Colonnes du fichier brut
COLUMN_NAMES = [
    'date', 'time',
    'pm1_0_std', 'pm2_5_std', 'pm10_std',
    'pm1_0_atm', 'pm2_5_atm', 'pm10_atm',
    'particles_03', 'particles_05', 'particles_10',
    'particles_25', 'particles_50', 'particles_100'
]

# ============================================================
# UDFs
# ============================================================
@udf(IntegerType())
def calculate_aqi(pm25):
    if pm25 is None: return 0
    breakpoints = [
        (0.0, 12.0, 0, 50), (12.1, 35.4, 51, 100), (35.5, 55.4, 101, 150),
        (55.5, 150.4, 151, 200), (150.5, 250.4, 201, 300), (250.5, 500.4, 301, 500),
    ]
    for c_low, c_high, aqi_low, aqi_high in breakpoints:
        if c_low <= pm25 <= c_high:
            return int(round(((aqi_high - aqi_low) / (c_high - c_low)) * (pm25 - c_low) + aqi_low))
    return 500 if pm25 > 500.4 else 0

@udf(StringType())
def get_aqi_category(aqi):
    if aqi is None: return "Inconnu"
    if aqi <= 50: return "Bon"
    elif aqi <= 100: return "Modéré"
    elif aqi <= 150: return "Mauvais pour sensibles"
    elif aqi <= 200: return "Mauvais"
    elif aqi <= 300: return "Très mauvais"
    return "Dangereux"

@udf(StringType())
def get_aqi_color(aqi):
    if aqi is None: return "#808080"
    if aqi <= 50: return "#00E400"
    elif aqi <= 100: return "#FFFF00"
    elif aqi <= 150: return "#FF7E00"
    elif aqi <= 200: return "#FF0000"
    elif aqi <= 300: return "#8F3F97"
    return "#7E0023"

@udf(StringType())
def get_aqi_recommendation(aqi):
    if aqi is None: return "Données indisponibles"
    if aqi <= 50: return "Qualité satisfaisante"
    elif aqi <= 100: return "Acceptable"
    elif aqi <= 150: return "Sensibles : limiter efforts"
    elif aqi <= 200: return "Éviter efforts prolongés"
    elif aqi <= 300: return "Rester à l'intérieur"
    return "Urgence sanitaire"

@udf(DoubleType())
def calculate_quality_percent(aqi):
    if aqi is None or aqi <= 0: return 100.0
    elif aqi >= 300: return 0.0
    return round(100 * (1 - (aqi / 300) ** 0.7), 1)

@udf(StringType())
def get_period(hour_val):
    if hour_val is None: return "Inconnu"
    if 6 <= hour_val < 12: return "Matin"
    elif 12 <= hour_val < 14: return "Midi"
    elif 14 <= hour_val < 18: return "Après-midi"
    elif 18 <= hour_val < 22: return "Soir"
    return "Nuit"

@udf(StringType())
def get_day_name(dow):
    days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    if dow is None or dow < 0 or dow > 6: return "Inconnu"
    return days[dow]

# ============================================================
# TRAITEMENT PRINCIPAL
# ============================================================
def process_batch(input_file, output_file=None):
    """
    Traite un fichier batch et applique toutes les transformations.
    
    Args:
        input_file: Chemin vers le fichier CSV brut (format capteur)
        output_file: Chemin de sortie (optionnel)
    """
    print("=" * 60)
    print("🌬️ Test Local - Air Quality Processing")
    print("=" * 60)
    
    # Créer la session Spark
    spark = SparkSession.builder \
        .appName("AirQuality_Test_Local") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    print("✅ Session Spark créée")
    
    # Schéma des données brutes
    schema = StructType([
        StructField("date", StringType(), True),
        StructField("time", StringType(), True),
        StructField("pm1_0_std", DoubleType(), True),
        StructField("pm2_5_std", DoubleType(), True),
        StructField("pm10_std", DoubleType(), True),
        StructField("pm1_0_atm", DoubleType(), True),
        StructField("pm2_5_atm", DoubleType(), True),
        StructField("pm10_atm", DoubleType(), True),
        StructField("particles_03", IntegerType(), True),
        StructField("particles_05", IntegerType(), True),
        StructField("particles_10", IntegerType(), True),
        StructField("particles_25", IntegerType(), True),
        StructField("particles_50", IntegerType(), True),
        StructField("particles_100", IntegerType(), True),
    ])
    
    # Charger les données
    print(f"📂 Chargement de: {input_file}")
    df = spark.read.csv(input_file, schema=schema, header=False)
    print(f"   → {df.count()} lignes chargées")
    
    # Créer datetime et heure
    df = df.withColumn("datetime", to_timestamp(concat_ws(" ", col("date"), col("time"))))
    df = df.withColumn("hour", hour(col("datetime")))
    
    # Créer l'array des zones
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
    
    # Explode pour créer une ligne par zone
    df = df.withColumn("zone", explode(zones_array))
    
    # Extraire les colonnes de zone
    df = df.select(
        "*",
        col("zone.zone_id").alias("zone_id"),
        col("zone.zone_name").alias("zone_name"),
        col("zone.latitude").alias("latitude"),
        col("zone.longitude").alias("longitude"),
        col("zone.pm_factor").alias("pm_factor"),
        col("zone.variability").alias("variability"),
        col("zone.rush_hour_boost").alias("rush_hour_boost"),
        col("zone.night_reduction").alias("night_reduction"),
    ).drop("zone")
    
    print("✅ Données augmentées pour 5 zones")
    
    # Calculer les facteurs
    df = df.withColumn("is_rush_hour",
        when((col("hour") >= 7) & (col("hour") <= 9), True)
        .when((col("hour") >= 17) & (col("hour") <= 19), True)
        .otherwise(False)
    ).withColumn("is_night",
        when((col("hour") >= 22) | (col("hour") <= 5), True)
        .otherwise(False)
    )
    
    # Facteur total
    df = df.withColumn("total_factor",
        col("pm_factor") * 
        when(col("is_rush_hour"), col("rush_hour_boost")).otherwise(lit(1.0)) *
        when(col("is_night"), col("night_reduction")).otherwise(lit(1.0)) *
        (lit(1.0) + (rand() * 2 - 1) * col("variability"))
    )
    
    # Appliquer aux PM
    df = df.withColumn("pm1_0_atm_aug", spark_round(col("pm1_0_atm") * col("total_factor"), 1))
    df = df.withColumn("pm2_5_atm_aug", spark_round(col("pm2_5_atm") * col("total_factor"), 1))
    df = df.withColumn("pm10_atm_aug", spark_round(col("pm10_atm") * col("total_factor"), 1))
    
    # Facteur particules
    particle_factor = col("pm_factor") * when(col("is_rush_hour"), col("rush_hour_boost")).otherwise(lit(1.0))
    df = df.withColumn("particles_03_aug", (col("particles_03") * particle_factor).cast("int"))
    df = df.withColumn("particles_05_aug", (col("particles_05") * particle_factor).cast("int"))
    df = df.withColumn("particles_10_aug", (col("particles_10") * particle_factor).cast("int"))
    df = df.withColumn("particles_25_aug", (col("particles_25") * particle_factor).cast("int"))
    df = df.withColumn("particles_50_aug", (col("particles_50") * particle_factor).cast("int"))
    df = df.withColumn("particles_100_aug", (col("particles_100") * particle_factor).cast("int"))
    
    print("✅ Facteurs de zone appliqués")
    
    # Calculer AQI
    df = df.withColumn("aqi", calculate_aqi(col("pm2_5_atm_aug")))
    df = df.withColumn("aqi_category", get_aqi_category(col("aqi")))
    df = df.withColumn("aqi_color", get_aqi_color(col("aqi")))
    df = df.withColumn("aqi_recommendation", get_aqi_recommendation(col("aqi")))
    df = df.withColumn("air_quality_percent", calculate_quality_percent(col("aqi")))
    
    print("✅ AQI calculé")
    
    # Enrichissement temporel
    # Correction: Spark dayofweek() retourne 1=Dimanche...7=Samedi
    # Python weekday() retourne 0=Lundi...6=Dimanche
    # Formule de conversion: (dayofweek + 5) % 7
    df = df.withColumn("day_of_week", ((dayofweek(col("datetime")) + 5) % 7).cast("int"))
    df = df.withColumn("day_name", get_day_name(col("day_of_week")))
    df = df.withColumn("month", month(col("datetime")))
    df = df.withColumn("is_weekend", when(col("day_of_week").isin([5, 6]), 1).otherwise(0))
    df = df.withColumn("period", get_period(col("hour")))
    df = df.withColumn("sport_ok", when(col("aqi") <= 100, True).otherwise(False))
    
    print("✅ Enrichissement temporel appliqué")
    
    # Sélection finale
    final_df = df.select(
        "datetime", "zone_id", "zone_name", "latitude", "longitude",
        col("pm1_0_atm_aug").alias("pm1_0_atm"),
        col("pm2_5_atm_aug").alias("pm2_5_atm"),
        col("pm10_atm_aug").alias("pm10_atm"),
        col("particles_03_aug").alias("particles_03"),
        col("particles_05_aug").alias("particles_05"),
        col("particles_10_aug").alias("particles_10"),
        col("particles_25_aug").alias("particles_25"),
        col("particles_50_aug").alias("particles_50"),
        col("particles_100_aug").alias("particles_100"),
        "aqi", "aqi_category", "aqi_color", "aqi_recommendation",
        "air_quality_percent", "hour", "date", "time",
        "day_of_week", "day_name", "month", "is_weekend", "period", "sport_ok"
    )
    
    # Afficher les statistiques
    print("\n" + "=" * 60)
    print("📊 STATISTIQUES PAR ZONE")
    print("=" * 60)
    
    stats = final_df.groupBy("zone_name").agg({
        "aqi": "avg",
        "pm2_5_atm": "avg",
        "air_quality_percent": "avg"
    }).orderBy("avg(aqi)")
    
    stats.show(truncate=False)
    
    # Exporter si demandé
    if output_file:
        print(f"\n📁 Export vers: {output_file}")
        final_df.coalesce(1).write.mode("overwrite").csv(output_file, header=True)
        print("✅ Export terminé")
    
    # Afficher un échantillon
    print("\n📋 Échantillon des données traitées:")
    final_df.select(
        "datetime", "zone_name", "pm2_5_atm", "aqi", "aqi_category", "period"
    ).show(20, truncate=False)
    
    total_rows = final_df.count()
    print(f"\n✅ Traitement terminé: {total_rows} lignes générées")
    
    spark.stop()
    return final_df

# ============================================================
# MAIN
# ============================================================
if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python spark_test_local.py <input_file.txt> [output_file]")
        print("\nExemple:")
        print("  python spark_test_local.py data_pfe.txt output_processed")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else None
    
    process_batch(input_file, output_file)