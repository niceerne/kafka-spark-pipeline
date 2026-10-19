import json
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

TOPIC = "ecommerce-events"


def connect():
    while True:
        try:
            return KafkaProducer(
                bootstrap_servers="kafka:9092",
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
        except NoBrokersAvailable:
            print("Kafka pas encore prêt, nouvelle tentative dans 3 s...")
            time.sleep(3)


producer = connect()

PRODUCTS = {f"P{i}": round(random.uniform(5, 300), 2) for i in range(1, 51)}
# Entonnoir réaliste : beaucoup de vues, moins d'ajouts au panier, peu d'achats
EVENTS = ["view"] * 70 + ["add_to_cart"] * 20 + ["purchase"] * 10

print("Producteur démarré, envoi des événements...")
while True:
    pid = random.choice(list(PRODUCTS))
    event = {
        "event_id": str(uuid.uuid4()),
        "user_id": f"U{random.randint(1, 500)}",
        "product_id": pid,
        "event_type": random.choice(EVENTS),
        "price": PRODUCTS[pid],
        "ts": datetime.now(timezone.utc).isoformat(),
    }
    producer.send(TOPIC, event)
    time.sleep(random.uniform(0.05, 0.3))
