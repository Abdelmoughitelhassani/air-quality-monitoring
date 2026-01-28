-- ============================================================
-- 🌬️ Air Quality Database Schema
-- ============================================================
-- Ce script crée les tables pour stocker les données de qualité de l'air
-- Exécuté automatiquement au démarrage du conteneur PostgreSQL

-- ============================================================
-- Table: sensor_readings (Données par capteur virtuel)
-- ============================================================
CREATE TABLE IF NOT EXISTS sensor_readings (
    id SERIAL PRIMARY KEY,
    
    -- Identifiants
    sensor_id VARCHAR(50) NOT NULL,
    zone_id VARCHAR(50) NOT NULL,
    zone_name VARCHAR(100) NOT NULL,
    
    -- Localisation
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    
    -- Temporel
    datetime TIMESTAMP NOT NULL,
    date VARCHAR(10),
    time VARCHAR(8),
    hour INTEGER,
    
    -- Mesures PM (µg/m³)
    pm1_0_atm DOUBLE PRECISION,
    pm2_5_atm DOUBLE PRECISION,
    pm10_atm DOUBLE PRECISION,
    
    -- Comptage particules (par 0.1L)
    particles_03 INTEGER,
    particles_05 INTEGER,
    particles_10 INTEGER,
    particles_25 INTEGER,
    particles_50 INTEGER,
    particles_100 INTEGER,
    
    -- AQI
    aqi INTEGER,
    aqi_category VARCHAR(50),
    aqi_color VARCHAR(10),
    aqi_recommendation VARCHAR(100),
    air_quality_percent DOUBLE PRECISION,
    
    -- Enrichissement temporel
    day_of_week INTEGER,
    day_name VARCHAR(15),
    month INTEGER,
    is_weekend INTEGER,
    period VARCHAR(20),
    
    -- Sport
    sport_ok BOOLEAN,
    
    -- Métadonnées
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index pour les requêtes fréquentes
CREATE INDEX IF NOT EXISTS idx_sensor_readings_sensor_id ON sensor_readings(sensor_id);
CREATE INDEX IF NOT EXISTS idx_sensor_readings_zone_id ON sensor_readings(zone_id);
CREATE INDEX IF NOT EXISTS idx_sensor_readings_datetime ON sensor_readings(datetime);
CREATE INDEX IF NOT EXISTS idx_sensor_readings_aqi ON sensor_readings(aqi);
CREATE INDEX IF NOT EXISTS idx_sensor_readings_date ON sensor_readings(date);

-- ============================================================
-- Table: zone_averages (Moyennes par zone)
-- ============================================================
CREATE TABLE IF NOT EXISTS zone_averages (
    id SERIAL PRIMARY KEY,
    
    -- Fenêtre temporelle
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    
    -- Zone
    zone_id VARCHAR(50) NOT NULL,
    zone_name VARCHAR(100) NOT NULL,
    
    -- Capteurs actifs
    active_sensors INTEGER,
    
    -- Localisation (centre de la zone)
    zone_latitude DOUBLE PRECISION,
    zone_longitude DOUBLE PRECISION,
    
    -- Moyennes PM
    avg_pm1_0 DOUBLE PRECISION,
    avg_pm2_5 DOUBLE PRECISION,
    avg_pm10 DOUBLE PRECISION,
    min_pm2_5 DOUBLE PRECISION,
    max_pm2_5 DOUBLE PRECISION,
    
    -- Moyennes AQI
    avg_aqi INTEGER,
    min_aqi INTEGER,
    max_aqi INTEGER,
    
    -- Catégorie zone
    zone_aqi_category VARCHAR(50),
    zone_aqi_color VARCHAR(10),
    zone_recommendation VARCHAR(100),
    
    -- Qualité air
    avg_air_quality_percent DOUBLE PRECISION,
    
    -- Liste capteurs (string CSV)
    sensor_ids_str TEXT,
    
    -- Métadonnées
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index pour les requêtes fréquentes
CREATE INDEX IF NOT EXISTS idx_zone_averages_zone_id ON zone_averages(zone_id);
CREATE INDEX IF NOT EXISTS idx_zone_averages_window_start ON zone_averages(window_start);
CREATE INDEX IF NOT EXISTS idx_zone_averages_avg_aqi ON zone_averages(avg_aqi);

-- ============================================================
-- Vues utiles
-- ============================================================

-- Vue: Dernières lectures par capteur
CREATE OR REPLACE VIEW latest_sensor_readings AS
SELECT DISTINCT ON (sensor_id)
    sensor_id,
    zone_id,
    zone_name,
    latitude,
    longitude,
    datetime,
    pm1_0_atm,
    pm2_5_atm,
    pm10_atm,
    aqi,
    aqi_category,
    aqi_color,
    air_quality_percent,
    sport_ok
FROM sensor_readings
ORDER BY sensor_id, datetime DESC;

-- Vue: Dernières moyennes par zone
CREATE OR REPLACE VIEW latest_zone_averages AS
SELECT DISTINCT ON (zone_id)
    zone_id,
    zone_name,
    window_start,
    window_end,
    active_sensors,
    zone_latitude,
    zone_longitude,
    avg_pm2_5,
    avg_aqi,
    zone_aqi_category,
    zone_aqi_color,
    avg_air_quality_percent
FROM zone_averages
ORDER BY zone_id, window_start DESC;

-- Vue: Statistiques quotidiennes par zone
CREATE OR REPLACE VIEW daily_zone_stats AS
SELECT 
    zone_id,
    zone_name,
    DATE(window_start) as date,
    COUNT(*) as readings_count,
    ROUND(AVG(avg_pm2_5)::numeric, 1) as daily_avg_pm25,
    ROUND(AVG(avg_aqi)::numeric, 0) as daily_avg_aqi,
    MIN(min_aqi) as daily_min_aqi,
    MAX(max_aqi) as daily_max_aqi
FROM zone_averages
GROUP BY zone_id, zone_name, DATE(window_start)
ORDER BY date DESC, zone_id;

-- ============================================================
-- Fonctions utiles
-- ============================================================

-- Fonction: Obtenir la catégorie AQI
CREATE OR REPLACE FUNCTION get_aqi_category(aqi_value INTEGER)
RETURNS VARCHAR(50) AS $$
BEGIN
    IF aqi_value <= 50 THEN RETURN 'Bon';
    ELSIF aqi_value <= 100 THEN RETURN 'Modéré';
    ELSIF aqi_value <= 150 THEN RETURN 'Mauvais pour sensibles';
    ELSIF aqi_value <= 200 THEN RETURN 'Mauvais';
    ELSIF aqi_value <= 300 THEN RETURN 'Très mauvais';
    ELSE RETURN 'Dangereux';
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Fonction: Obtenir la couleur AQI
CREATE OR REPLACE FUNCTION get_aqi_color(aqi_value INTEGER)
RETURNS VARCHAR(10) AS $$
BEGIN
    IF aqi_value <= 50 THEN RETURN '#00E400';
    ELSIF aqi_value <= 100 THEN RETURN '#FFFF00';
    ELSIF aqi_value <= 150 THEN RETURN '#FF7E00';
    ELSIF aqi_value <= 200 THEN RETURN '#FF0000';
    ELSIF aqi_value <= 300 THEN RETURN '#8F3F97';
    ELSE RETURN '#7E0023';
    END IF;
END;
$$ LANGUAGE plpgsql;

-- ============================================================
-- Permissions
-- ============================================================
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO airquality_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO airquality_user;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO airquality_user;

-- ============================================================
-- Message de confirmation
-- ============================================================
DO $$
BEGIN
    RAISE NOTICE '✅ Base de données Air Quality initialisée avec succès!';
    RAISE NOTICE '   Tables: sensor_readings, zone_averages';
    RAISE NOTICE '   Vues: latest_sensor_readings, latest_zone_averages, daily_zone_stats';
END $$;