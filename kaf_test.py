# # test_kafka.py (create this file)
# from kafka import KafkaProducer
# import json

# producer = KafkaProducer(
#     bootstrap_servers='localhost:9092',
#     retries=10,
#     request_timeout_ms=120000,
#     max_block_ms=120000
# )

# future = producer.send('orders_topic', {'test': 'alive'})
# result = future.get(timeout=30)
# print(f"✅ SUCCESS: offset={result.offset}")
# producer.close()

import json
from kafka import KafkaProducer
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

event = {
    "order_id": "ORD123",
    "customer_id": "CUST9",
    "product_id": "PROD7",
    "anomaly_type": "DISCOUNT_TOO_HIGH",
    "price": 500,
    "final_price": 100,
    "detected_at": datetime.utcnow().isoformat()
}

producer.send("anomalies.detected", event)
producer.flush()

print("✅ Test anomaly sent to Kafka")