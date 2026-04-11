from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

for message in consumer:
    tx = message.value

    if tx['amount'] > 3000:

        tx['risk_level'] = "High"
    elif tx['amount'] > 1000:

        tx['risk_level'] = "Medium"
    else:
        tx['risk_level'] = "Low"
        
    print(f" {tx['risk_level']} ID:{tx['tx_id']} | {tx['amount']:.2f} PLN | {tx['store']} | {tx['category']} | {tx['is_anomaly']}")
    
