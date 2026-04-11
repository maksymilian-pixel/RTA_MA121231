from kafka import KafkaConsumer, KafkaProducer
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)
producer = KafkaProducer(
    bootstrap_servers='broker:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)


for message in consumer:
    tx = message.value

    if tx['amount'] > 1000:

        producer.send('filtered_transactions', value=tx)
        print(f" Alert!")
        print(f"ID: {tx['tx_id']} | {tx['amount']:.2f} PLN | {tx['store']} | {tx['is_anomaly']}")
    
# TWÓJ KOD
# Dla każdej wiadomości: sprawdź amount > 1000, jeśli tak — wypisz ALERT

