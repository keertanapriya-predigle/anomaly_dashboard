import os, sys, json
from kafka import KafkaConsumer
from django.utils import timezone

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(BASE_DIR, "anomaly_backend"))

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "anomaly_backend.settings")

import django
django.setup()

from anomalies.models import Anomaly

consumer = KafkaConsumer(
    "anomalies.detected",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda x: json.loads(x.decode()),
    group_id="db-consumer",
    auto_offset_reset="earliest"
)

print("🗄️ DB Consumer started")

for msg in consumer:
    e = msg.value

    Anomaly.objects.create(
        order_id=e["order_id"],
        customer_id=e["customer_id"],
        product_id=e["product_id"],
        anomaly_type="ML Price Anomaly",
        details=e,
        detected_at=timezone.now()
    )

    print("✅ Saved to PostgreSQL")