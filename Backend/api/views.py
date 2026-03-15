# views.py
import threading
from collections import defaultdict
from datetime import datetime
from rest_framework.decorators import api_view
from rest_framework.response import Response
from confluent_kafka import Consumer, KafkaError
import json
import pandas as pd
import os

# ────────────────────────────────────────────────────────────────
# Structure de données en mémoire (la plus utilisée)
# ────────────────────────────────────────────────────────────────

# 1. Dernière mesure par zone (le plus rapide pour la carte + zones list)
latest_by_zone = {}                    # zone_id → dict de la dernière mesure

# 2. Historique récent par zone (pour /history/)
history_by_zone = defaultdict(list)    # zone_id → liste des 100-500 dernières mesures

# 3. Verrou pour éviter les corruptions lors des mises à jour concurrentes
data_lock = threading.Lock()

# ────────────────────────────────────────────────────────────────
# Consommateur Kafka en arrière-plan
# ────────────────────────────────────────────────────────────────

def kafka_consumer_thread():
    consumer_conf = {
        'bootstrap.servers': os.environ.get('KAFKA_BOOTSTRAP_SERVERS'),
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': os.environ.get('KAFKA_SASL_USERNAME'),
        'sasl.password': os.environ.get('KAFKA_SASL_PASSWORD'),
        'group.id': 'air-quality-backend-consumer',
        'auto.offset.reset': 'latest',          # on commence par les messages récents
        'enable.auto.commit': True,
    }

    consumer = Consumer(consumer_conf)
    consumer.subscribe(['topic_4'])

    print("→ Kafka consumer démarré (topic: topic_4)")

    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Erreur Kafka: {msg.error()}")
            continue

        try:
            payload = json.loads(msg.value().decode('utf-8'))
            zone_id = payload.get('zone_id')
            if not zone_id:
                print("Message ignoré : pas de zone_id")
                continue

            payload['received_at'] = datetime.utcnow().isoformat()

            with data_lock:
                # Mise à jour dernière mesure
                latest_by_zone[zone_id] = payload
                print(f"✓ Dernière mesure mise à jour pour {zone_id} (AQI: {payload.get('aqi')})")

                # Ajout à l'historique
                history_by_zone[zone_id].append(payload)
                print(f"→ Ajout à l'historique de {zone_id} → {len(history_by_zone[zone_id])} mesures maintenant")

                # Limite à 500
                if len(history_by_zone[zone_id]) > 500:
                    history_by_zone[zone_id] = history_by_zone[zone_id][-500:]

        except Exception as e:
            print(f"Erreur traitement message: {e}")

    consumer.close()  # jamais atteint en pratique


# Lancement du consommateur au démarrage (une seule fois)
if not hasattr(kafka_consumer_thread, 'started'):
    threading.Thread(target=kafka_consumer_thread, daemon=True).start()
    kafka_consumer_thread.started = True


# ────────────────────────────────────────────────────────────────
# Endpoints API adaptés (sans pandas pour les cas les plus fréquents)
# ────────────────────────────────────────────────────────────────

@api_view(['GET'])
def zones_list(request):
    """Liste toutes les zones avec leur dernière mesure"""
    with data_lock:
        result = []
        for zone_id, measure in latest_by_zone.items():
            result.append({
                'zone_id': zone_id,
                'zone_name': measure.get('zone_name', 'Inconnu'),
                'latitude': float(measure.get('latitude', 0)),
                'longitude': float(measure.get('longitude', 0)),
                'latest_aqi': {
                    'aqi': int(measure.get('aqi', 0)),
                    'category': measure.get('aqi_category', 'Inconnu'),
                    'color': measure.get('aqi_color', '#888888'),
                    'pm2_5': float(measure.get('pm2_5_atm', 0)),
                    'pm10': float(measure.get('pm10_atm', 0)),
                },
                'datetime': measure.get('datetime')
            })
    
    return Response(result)


@api_view(['GET'])
def map_data(request):
    """Données optimisées pour la carte"""
    with data_lock:
        result = []
        for zone_id, m in latest_by_zone.items():
            result.append({
                'zone_id': zone_id,
                'zone_name': m.get('zone_name'),
                'latitude': float(m.get('latitude', 0)),
                'longitude': float(m.get('longitude', 0)),
                'aqi': int(m.get('aqi', 0)),
                'aqi_category': m.get('aqi_category'),
                'aqi_color': m.get('aqi_color'),
                'pm2_5': float(m.get('pm2_5_atm', 0)),
                'pm10': float(m.get('pm10_atm', 0)),
                'datetime': m.get('datetime'),
                'recommendation': m.get('aqi_recommendation', '')
            })
    
    return Response(result)


@api_view(['GET'])
def zone_history(request, zone_id):
    """Historique récent d'une zone"""
    hours = int(request.GET.get('hours', 24))

    with data_lock:
        measures = history_by_zone.get(zone_id, [])
    
    if not measures:
        return Response({'error': 'Aucune donnée pour cette zone'}, status=404)

    # Conversion en DataFrame uniquement si nécessaire (pour filtrer par heures)
    df = pd.DataFrame(measures)
    df['datetime'] = pd.to_datetime(df['datetime'])
    
    since = datetime.utcnow() - pd.Timedelta(hours=hours)
    df = df[df['datetime'] >= since]
    df = df.sort_values('datetime', ascending=False)

    result = df.to_dict('records')
    return Response(result)


@api_view(['GET'])
def latest_measurements(request):
    """Dernières mesures pour toutes les zones"""
    with data_lock:
        result = list(latest_by_zone.values())
    
    return Response(result)

# Ajoute ces fonctions dans views.py (juste après les autres)

@api_view(['GET'])
def measurements_list(request):
    """Liste des mesures récentes avec filtres (limité à 100 par défaut)"""
    zone_id = request.GET.get('zone_id')
    hours = request.GET.get('hours', 24)

    with data_lock:
        if zone_id:
            measures = history_by_zone.get(zone_id, [])
        else:
            # Toutes les zones → on prend les 100 dernières de chaque (limité)
            measures = []
            for zone_measures in history_by_zone.values():
                measures.extend(zone_measures[-100:])  # 100 par zone max

    if not measures:
        return Response([])

    df = pd.DataFrame(measures)
    df['datetime'] = pd.to_datetime(df['datetime'])

    if hours:
        since = datetime.utcnow() - pd.Timedelta(hours=int(hours))
        df = df[df['datetime'] >= since]

    df = df.sort_values('datetime', ascending=False).head(100)
    result = df.to_dict('records')

    return Response(result)


@api_view(['GET'])
def latest_measurements(request):
    """Dernières mesures pour toutes les zones"""
    with data_lock:
        result = list(latest_by_zone.values())
    
    # Optionnel : tri par datetime si présent
    if result and 'datetime' in result[0]:
        result.sort(key=lambda x: x.get('datetime', ''), reverse=True)
    
    return Response(result)


@api_view(['GET'])
def stats(request):
    """Statistiques agrégées sur les dernières heures"""
    hours = int(request.GET.get('hours', 24))
    since = datetime.utcnow() - pd.Timedelta(hours=hours)

    with data_lock:
        all_measures = []
        for measures in history_by_zone.values():
            all_measures.extend(measures)

    if not all_measures:
        return Response([])

    df = pd.DataFrame(all_measures)
    df['datetime'] = pd.to_datetime(df['datetime'])
    df = df[df['datetime'] >= since]

    if df.empty:
        return Response([])

    stats = df.groupby(['zone_id', 'zone_name']).agg({
        'aqi': ['mean', 'min', 'max'],
        'pm2_5_atm': 'mean',
        'pm10_atm': 'mean',
    }).reset_index()

    stats.columns = ['zone_id', 'zone_name', 'avg_aqi', 'min_aqi', 'max_aqi', 
                     'avg_pm2_5', 'avg_pm10']

    result = stats.to_dict('records')
    return Response(result)