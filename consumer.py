from confluent_kafka import Consumer, KafkaException, KafkaError
import os
# Configuration du consumer pour Confluent Cloud
conf = {
    'bootstrap.servers': os.environ.get('KAFKA_BOOTSTRAP_SERVERS'),
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': os.environ.get('KAFKA_SASL_USERNAME'),
    'sasl.password': os.environ.get('KAFKA_SASL_PASSWORD'),

    # IMPORTANT POUR CONSUMER
    'group.id': 'python-consumer-group-1',   # nom de groupe → change si tu veux repartitionner
    'auto.offset.reset': 'latest',         # lit depuis le début si pas de commit existant
}

consumer = Consumer(conf)

TOPIC = "air_quality_sensors3"   # 🔥 change comme ton producer

consumer.subscribe([TOPIC])
print("📥 En attente des messages... Ctrl+C pour arrêter\n")

try:
    while True:
        msg = consumer.poll(1.0)  # attend un message pendant 1 seconde

        if msg is None:
            continue

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                raise KafkaException(msg.error())

        print(f"👉 Message reçu : {msg.value().decode('utf-8')}  "
              f"(partition {msg.partition()}, offset {msg.offset()})")

except KeyboardInterrupt:
    print("\n⛔ Arrêt du consumer")

finally:
    consumer.close()
    print("✔️ Consumer fermé proprement.")
