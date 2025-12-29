# -*- coding: utf-8 -*-
"""
Created on Wed Dec 24 11:36:44 2025
@author: keertanapriya
"""
import pandas as pd
from kafka import KafkaProducer
import json
import time

print("---Starting Kafka Producer---")

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    retries=3,
    acks='all'
)

df = pd.read_csv("ecommerce_dataset_updated.csv")
print(f"Loaded {len(df)} records from CSV")

for i, (_, row) in enumerate(df.iterrows()):
    try:
        # Send & WAIT for confirmation (key fix!)
        future = producer.send("orders_topic", row.to_dict())
        result = future.get(timeout=10)  # Block until delivered
        
        print(f"Sent record {i+1}/{len(df)} to partition {result.partition} "
              f"offset={result.offset} | Order: {row.get('order_id', 'N/A')}")
        
    except Exception as e:
        print(f"❌ Error sending record {i+1}: {e}")
    
    time.sleep(20)

# CRITICAL: Force send all queued messages
print("Flushing remaining messages...")
producer.flush()
print("All messages delivered!")
print("-------------------------------")
producer.close()
