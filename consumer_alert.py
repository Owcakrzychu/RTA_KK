from kafka import KafkaConsumer
import json
from datetime import datetime, timedelta
from collections import defaultdict

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='latest',
    group_id='fraud-detection-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

user_tx_times = defaultdict(list)

TIME_WINDOW_SEC = 60
MAX_TRANSACTIONS = 3

print("Rozpoczęto nasłuchiwanie w poszukiwaniu anomalii (więcej niż 3 transakcje w 60s)...")

for message in consumer:
    tx = message.value
    user_id = tx['user_id']
    
    try:
        current_tx_time = datetime.fromisoformat(tx['timestamp'])
    except ValueError:
        continue
        
    user_tx_times[user_id].append(current_tx_time)

    cutoff_time = current_tx_time - timedelta(seconds=TIME_WINDOW_SEC)

    user_tx_times[user_id] = [t for t in user_tx_times[user_id] if t >= cutoff_time]

    current_tx_count = len(user_tx_times[user_id])
    
    if current_tx_count > MAX_TRANSACTIONS:
        print(f"ALERT! Użytkownik {user_id} wykonał {current_tx_count} transakcje w ciągu 60 sekund!")

        user_tx_times[user_id].clear()
