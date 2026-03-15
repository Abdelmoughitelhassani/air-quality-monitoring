# 🌬️ Système Intelligent de Surveillance de la Qualité de l'Air

> Projet de Fin d'Études (PFE) — El Hassani Abdelmoughit & Boulghalegh Youssef

Pipeline temps réel de collecte, traitement et visualisation de données de qualité de l'air dans la région de Montbéliard, basé sur un capteur physique **PMS5003** et une architecture Big Data complète.

---

## Architecture du système

```
Capteur PMS5003
      │
      ▼
 Apache Kafka          ← Confluent Cloud (cloud)
(Confluent Cloud)
      │
      ▼
 Apache Spark          ← Structured Streaming
(Spark Master + Workers)
      │
      ├──► PostgreSQL  ← Persistance des données
      │
      └──► Kafka Topics ← air_quality_sensors / air_quality_zones
                │
                ▼
          Django REST API
                │
                ▼
        Application Mobile
          (React Native / Expo)
```

---

## Stack technique

| Composant | Technologie |
|-----------|-------------|
| Ingestion | Apache Kafka (Confluent Cloud) |
| Traitement | Apache Spark 3.5 (Structured Streaming) |
| Base de données | PostgreSQL |
| Backend API | Django REST Framework |
| Frontend Mobile | React Native (Expo) |
| Infrastructure | Docker & Docker Compose |
| Langage | Python 3.12 |

---

## Structure du projet

```
pfe_project/
├── app/
│   └── spark_streaming_virtual_sensors.py   # Job Spark principal
├── Backend/
│   ├── api/
│   │   ├── views.py        # Endpoints REST
│   │   └── urls.py
│   └── config/
│       └── settings.py     # Config Django
├── mobile_django/           # App React Native (Expo)
├── data/
│   └── data_pfe.txt        # Données capteur brutes
├── producer.py              # Producer Kafka
├── consumer.py              # Consumer Kafka (test)
├── regrouper_date.py        # Prétraitement des données
├── init-postgres.sql        # Schéma base de données
├── docker-compose.yml       # Infrastructure complète
├── Dockerfile               # Image Spark custom
├── requirements.txt
└── .env.example             # Template des variables d'environnement
```

---

## Fonctionnement

1. Le capteur **PMS5003** mesure les particules fines (PM1.0, PM2.5, PM10) en temps réel
2. Le **Producer Kafka** envoie les mesures vers Confluent Cloud
3. **Spark Structured Streaming** consomme le flux et :
   - Génère **65 capteurs virtuels** répartis sur **5 zones** autour de Montbéliard
   - Calcule l'**AQI** (Air Quality Index) pour chaque capteur
   - Calcule les **moyennes par zone** via des fenêtres temporelles
   - Écrit les résultats dans PostgreSQL et republié dans Kafka
4. L'**API Django** expose les données en REST pour la carte et l'historique
5. L'**app mobile** affiche une carte interactive avec les alertes par zone

---

## Installation

### Prérequis

- Docker & Docker Compose
- Un compte [Confluent Cloud](https://confluent.io) (plan gratuit possible)
- Python 3.12+

### 1. Cloner le projet

```bash
git clone https://github.com/votre-username/air-quality-monitoring.git
cd air-quality-monitoring
```

### 2. Configurer les variables d'environnement

```bash
cp .env.example .env
```

Remplir `.env` avec vos vraies valeurs :

```env
KAFKA_BOOTSTRAP_SERVERS=pkc-xxxx.us-east-2.aws.confluent.cloud:9092
KAFKA_SASL_USERNAME=votre_api_key
KAFKA_SASL_PASSWORD=votre_api_secret
POSTGRES_PASSWORD=votre_mot_de_passe
DJANGO_SECRET_KEY=votre_cle_secrete
```

Générer une Django secret key :
```bash
python -c "from django.core.management.utils import get_random_secret_key; print(get_random_secret_key())"
```

### 3. Lancer l'infrastructure

```bash
docker-compose up -d
```

Services lancés :
- **Spark Master** → [http://localhost:8080](http://localhost:8080)
- **Spark Worker 1 & 2**
- **PostgreSQL** → `localhost:5432`
- **Django API** → [http://localhost:8000](http://localhost:8000)

### 4. Envoyer les données

```bash
# Installer les dépendances
pip install -r requirements.txt

# Lancer le producer
python producer.py
```

---

## API Endpoints

| Méthode | Endpoint | Description |
|---------|----------|-------------|
| GET | `/api/zones/` | Liste des zones avec dernière mesure |
| GET | `/api/map/` | Données pour la carte |
| GET | `/api/zones/<zone_id>/history/` | Historique d'une zone |
| GET | `/api/measurements/` | Mesures récentes |
| GET | `/api/stats/` | Statistiques agrégées |

---

## Schéma de base de données

Deux tables principales :

- **`sensor_readings`** — Données brutes par capteur virtuel (PM1.0, PM2.5, PM10, AQI, coordonnées GPS, horodatage)
- **`zone_averages`** — Moyennes calculées par zone sur des fenêtres temporelles

Vues créées automatiquement : `latest_sensor_readings`, `latest_zone_averages`, `daily_zone_stats`

---

## Calcul de l'AQI

L'AQI est calculé selon la norme US-EPA à partir du PM2.5 :

| AQI | Catégorie | Couleur |
|-----|-----------|---------|
| 0–50 | Bon | 🟢 |
| 51–100 | Modéré | 🟡 |
| 101–150 | Mauvais pour les sensibles | 🟠 |
| 151–200 | Mauvais | 🔴 |
| 201–300 | Très mauvais | 🟣 |
| 300+ | Dangereux | 🟤 |

---

## Auteurs

- **El Hassani Abdelmoughit** — [GitHub](https://github.com/Abdelmoughitelhassani)
- **Boulghalegh Youssef**
