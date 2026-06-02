#!/usr/bin/env python3
"""
Kafka Producer: Reads CSV and sends records in batches to Kafka
"""
import csv
import json
import time
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic

# Configuration
KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC_NAME = "ecommerce-transactions"
CSV_FILE = "data/ecommerce-transactions-raw.csv"
BATCH_SIZE = 5000  # Records per batch
DELAY_BETWEEN_BATCHES = 1  # Seconds

def create_topic_if_not_exists():
    """Create Kafka topic if it doesn't exist"""
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            client_id='topic_creator'
        )
        topics = admin_client.list_topics()
        if TOPIC_NAME not in topics:
            topic = NewTopic(name=TOPIC_NAME, num_partitions=3, replication_factor=1)
            admin_client.create_topics([topic])
            print(f"✅ Created topic: {TOPIC_NAME}")
        else:
            print(f"ℹ️  Topic {TOPIC_NAME} already exists")
        admin_client.close()
    except Exception as e:
        print(f"⚠️  Topic check: {e}")

def send_to_kafka():
    """Read CSV and send to Kafka in batches"""
    print("📖 Reading CSV file...")
    
    # Initialize Kafka producer
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks='all',
        retries=3,
        batch_size=16384,
        linger_ms=10
    )
    
    # Read CSV and send in batches
    with open(CSV_FILE, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        records = list(reader)
    
    total_records = len(records)
    print(f"📊 Total records: {total_records}")
    print(f"📦 Batch size: {BATCH_SIZE}")
    print("-" * 60)
    
    batches_sent = 0
    for i in range(0, total_records, BATCH_SIZE):
        batch = records[i:i + BATCH_SIZE]
        
        for record in batch:
            # Clean None values
            cleaned = {k: (v if v is not None else "") for k, v in record.items()}
            producer.send(TOPIC_NAME, value=cleaned)
        
        producer.flush()
        batches_sent += 1
        
        progress = min(i + BATCH_SIZE, total_records)
        percentage = (progress / total_records) * 100
        print(f"📨 Batch {batches_sent}: {progress}/{total_records} ({percentage:.1f}%)")
        
        time.sleep(DELAY_BETWEEN_BATCHES)
    
    producer.close()
    print(f"\n🎉 All {total_records} records sent to Kafka!")

if __name__ == "__main__":
    print("=" * 60)
    print("🚀 Starting Kafka Producer for E-Commerce Data")
    print("=" * 60)
    
    create_topic_if_not_exists()
    send_to_kafka()
