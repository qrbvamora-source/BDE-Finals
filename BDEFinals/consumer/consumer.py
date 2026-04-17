import json
import os
import time
from kafka import KafkaConsumer
from pymongo import MongoClient
from datetime import datetime

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
TOPIC_NAME = os.getenv('KAFKA_TOPIC', 'worldbank-data')
MONGO_URI = os.getenv('MONGO_URI', 'mongodb://localhost:27017')  # ✅ Will be overridden by the environment variable
DB_NAME = os.getenv('MONGO_DB', 'worldbank_db')
COLLECTION_NAME = os.getenv('MONGO_COLLECTION', 'indicator_history')

client = MongoClient(MONGO_URI, tlsAllowInvalidCertificates=True)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]
print(f"Connected to MongoDB Atlas at {MONGO_URI}")

max_retries = 12
retry_delay = 5

for attempt in range(max_retries):
    try:
        consumer = KafkaConsumer(
            TOPIC_NAME,
            bootstrap_servers=KAFKA_BROKER,
            auto_offset_reset='latest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        print(f"Consumer connected to Kafka at {KAFKA_BROKER}, listening to topic: {TOPIC_NAME}")
        break
    except Exception as e:
        print(f"Attempt {attempt+1}/{max_retries}: Kafka not ready. Retrying in {retry_delay}s...")
        time.sleep(retry_delay)
else:
    print("Failed to connect to Kafka. Exiting.")
    exit(1)

for msg in consumer:
    data = msg.value
    data['stored_at'] = datetime.utcnow()
    collection.insert_one(data)
    print(f"Stored: {data['country_name']} - {data['indicator_name']}: {data['value']} (Year: {data['year']})")