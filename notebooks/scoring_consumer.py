from kafka import KafkaConsumer, KafkaProducer
import json

consumer = KafkaConsumer('filtered_transactions', bootstrap_servers='broker:9092',
    auto_offset_reset='earliest', group_id='scoring-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')))

alert_producer = KafkaProducer(bootstrap_servers='broker:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# TWÓJ KOD
# Dla każdej transakcji: scoruj, jeśli >= 3: wyślij do 'alerts' i wypisz ALERT

def score_transaction(tx):
    score = 0
    rules = []

    amount = tx.get('amount', 0)
    category = str(tx.get('category', '')).lower()
    hour = tx.get('hour', 12)

    
    if amount > 3000:
        score += 3
        rules.append('R1')
        
    if amount > 1500 and category == 'elektronika':
        score += 2
        rules.append('R2')
        
    if hour < 6:
        score += 2
        rules.append('R3')
    
    return score, rules

for message in consumer:
    tx = message.value
    
    total_score, broken_rules = score_transaction(tx)
    tx['total_score'] = total_score
    tx['rules'] = broken_rules

    if total_score >= 3:
        print(f" ALERT ID: {tx['tx_id']} | Punkty: {total_score} | Reguły: {broken_rules}")
        
        alert_producer.send('alerts', value=tx)
        
