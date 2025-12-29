import os, sys, json
import asyncio
from kafka import KafkaConsumer
from channels.layers import get_channel_layer
from asgiref.sync import async_to_sync

# Django setup
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# Add Django project root (folder that contains manage.py)
sys.path.append(os.path.join(BASE_DIR, "anomaly_backend"))

# Point to the *inner* settings module
os.environ.setdefault(
    "DJANGO_SETTINGS_MODULE",
    "anomaly_backend.settings"
)


import django
django.setup()

channel_layer = get_channel_layer()

consumer = KafkaConsumer(
    "anomalies.detected",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda x: json.loads(x.decode()),
    group_id="ws-consumer",
    auto_offset_reset="latest"
)

print("📡 WebSocket Consumer started")

for msg in consumer:
    anomaly = msg.value
    print("🚨 Kafka anomaly received")

    async_to_sync(channel_layer.group_send)(
        "anomalies",
        {
            "type": "send_anomaly",
            "data": anomaly
        }
    )

    print("📤 Sent anomaly to WebSocket clients")
