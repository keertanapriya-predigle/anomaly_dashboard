import json
import numpy as np
import joblib
from kafka import KafkaConsumer, KafkaProducer

model = joblib.load("anomaly_model.pkl")

consumer = KafkaConsumer(
    "orders_topic",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda x: json.dumps(x).encode("utf-8")
)

print("🚀 Anomaly ML Producer running")

for msg in consumer:
    event = msg.value

    # 🔑 MATCH CSV COLUMN NAMES EXACTLY
    price = float(event["Price (Rs.)"])
    final_price = float(event["Final_Price(Rs.)"])

    discount_pct = ((price - final_price) / price) * 100 if price > 0 else 0

    features = np.array([[price, final_price]])
    ml_pred = model.predict(features)[0]

    print(f"Prediction={ml_pred} | ₹{price} → ₹{final_price}")

    # ✅ Hybrid detection (IMPORTANT)
    if ml_pred == -1 or discount_pct >= 50:
        anomaly_event = {
            "order_id": event.get("Order_ID"),
            "customer_id": event.get("User_ID"),
            "product_id": event.get("Product_ID"),
            "price": price,
            "final_price": final_price,
            "discount_pct": round(discount_pct, 2),
            "raw_event": event
        }

        producer.send("anomalies.detected", anomaly_event)
        producer.flush()

        print("🚨 Anomaly sent to Kafka")
