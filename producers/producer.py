import time
import json
import random
from datetime import datetime
from kafka import KafkaProducer

import os

# Configuration
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
TOPIC = 'clickstream'
PRODUCT_IDS = [f'prod_{i}' for i in range(1, 21)]  # 20 products
USER_IDS = [f'user_{i}' for i in range(1, 101)]    # 100 users
EVENT_TYPES = ['view', 'add_to_cart', 'purchase']
EVENT_WEIGHTS = [0.7, 0.2, 0.1]  # Probabilities for view, add_to_cart, purchase

def create_producer():
    producer = None
    while not producer:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print(f"Connected to Kafka at {KAFKA_BROKER}")
        except Exception as e:
            print(f"Failed to connect to Kafka at {KAFKA_BROKER}: {e}")
            print("Retrying in 5 seconds...")
            time.sleep(5)
    return producer

def generate_event():
    user_id = random.choice(USER_IDS)
    product_id = random.choice(PRODUCT_IDS)
    event_type = random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS, k=1)[0]
    timestamp = datetime.now().isoformat()
    
    return {
        'user_id': user_id,
        'product_id': product_id,
        'event_type': event_type,
        'timestamp': timestamp
    }

def main():
    producer = create_producer()
    if not producer:
        return

    print(f"Starting data generation to topic '{TOPIC}'...")
    try:
        while True:
            event = generate_event()
            producer.send(TOPIC, event)
            print(f"Sent: {event}")
            time.sleep(0.5)  # Simulate 2 events per second
    except KeyboardInterrupt:
        print("Stopping producer...")
    finally:
        producer.close()

if __name__ == "__main__":
    main()
