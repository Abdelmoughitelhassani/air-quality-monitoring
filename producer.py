from confluent_kafka import Producer
import pandas as pd
import time
import json

# Configuration du producer pour Confluent Cloud
import os

conf = {
    'bootstrap.servers': os.environ.get('KAFKA_BOOTSTRAP_SERVERS'),
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': os.environ.get('KAFKA_SASL_USERNAME'),
    'sasl.password': os.environ.get('KAFKA_SASL_PASSWORD'),
}

producer = Producer(conf)

def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Échec de livraison → {err}")
    else:
        print(f"✓ Envoyé | topic: {msg.topic()} | partition: {msg.partition()} | offset: {msg.offset()}")

# --------------------------------------
# CONFIGURATION
# --------------------------------------
CSV_FILE = "data_processed_final_sorted.csv"      # ← mets le chemin correct si nécessaire
TOPIC = "topic_4"                        # ← ton topic
DELAY_BETWEEN_MESSAGES = 7.0               # secondes entre chaque envoi

print(f"Lecture du fichier : {CSV_FILE}")
print(f"Envoi vers le topic : {TOPIC}")
print(f"Délai entre messages : {DELAY_BETWEEN_MESSAGES} secondes\n")

# Lecture du fichier CSV avec pandas
try:
    df = pd.read_csv(CSV_FILE)
    print(f"→ {len(df)} lignes trouvées dans le fichier\n")
except Exception as e:
    print(f"Erreur lors de la lecture du CSV : {e}")
    exit(1)

# On va envoyer chaque ligne sous forme de JSON (beaucoup plus propre et standard)
for index, row in df.iterrows():
    # Conversion de la ligne en dictionnaire puis en JSON
    row_dict = row.to_dict()
    
    # Optionnel : convertir datetime en string si besoin (pandas le fait souvent bien)
    if 'datetime' in row_dict:
        row_dict['datetime'] = str(row_dict['datetime'])

    # Transformation en JSON
    message = json.dumps(row_dict, ensure_ascii=False)
    
    # Envoi du message
    try:
        producer.produce(
            topic=TOPIC,
            value=message.encode('utf-8'),
            key=str(index).encode('utf-8'),           # optionnel : clé = numéro de ligne
            callback=delivery_report
        )
    except BufferError:
        print("Buffer plein ! On attend un peu...")
        producer.poll(1.0)
        producer.produce(TOPIC, value=message.encode('utf-8'), callback=delivery_report)

    # Traitement immédiat des callbacks
    producer.poll(0)

    print(f"  Ligne {index+1}/{len(df)} envoyée → attente {DELAY_BETWEEN_MESSAGES}s")
    time.sleep(DELAY_BETWEEN_MESSAGES)

# On attend que tous les messages partent vraiment
print("\nAttente de la fin de transmission...")
producer.flush(timeout=30)

print("✓ Tous les messages ont été envoyés (ou au moins transmis au broker)")