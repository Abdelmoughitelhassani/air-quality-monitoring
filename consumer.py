from confluent_kafka import Consumer, KafkaException, KafkaError

# Configuration du consumer pour Confluent Cloud
conf = {
    'bootstrap.servers': 'pkc-921jm.us-east-2.aws.confluent.cloud:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': 'ML4G7QKTNTKH5IBV',
    'sasl.password': 'cfltAGGAi+lzTeltLpNCH3aF3sCG5/pbE0TemK59vWMBZicFAd8sicKkeVQCbh8g',

    # IMPORTANT POUR CONSUMER
    'group.id': 'python-consumer-group-1',   # nom de groupe → change si tu veux repartitionner
    'auto.offset.reset': 'latest',         # lit depuis le début si pas de commit existant
}

consumer = Consumer(conf)

TOPIC = "topic_5"   # 🔥 change comme ton producer

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
