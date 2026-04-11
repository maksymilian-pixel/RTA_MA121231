from kafka import KafkaConsumer
from collections import Counter, defaultdict
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    group_id='count-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

store_counts = Counter()
total_amount = defaultdict(float)
msg_count = 0

# TWÓJ KOD
# Dla każdej wiadomości:
#   1. store_counts[store] += 1
#   2. total_amount[store] += amount
#   3. Co 10 wiadomości: print tabela
for message in consumer:
    tx = message.value
    store = tx['store']
    amount = tx['amount']
    
    store_counts[store] += 1
    total_amount[store] += amount
    msg_count += 1
    
    if msg_count % 10 == 0:
        print(f" Wynik (po {msg_count} wiadomościach)")
        print(f"{'SKLEP':<12} | {'LICZBA TX':<10} | {'SUMA PLN':<12}")
        print("-" * 40)
        
        for s in store_counts:
            count = store_counts[s]
            total = total_amount[s]
            print(f"{s:<12} | {count:<10} | {total:>10.2f} PLN")
        
        print("-" * 40)
