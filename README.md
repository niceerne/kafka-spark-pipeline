# Pipeline Big Data temps réel : Kafka & Spark

Pipeline de streaming e-commerce : des événements (consultations, ajouts au panier, achats) sont produits dans Apache Kafka, traités en temps réel par Spark Structured Streaming (agrégations par fenêtres temporelles), puis stockés dans PostgreSQL. Tout est déployé avec Docker Compose.

## Architecture

```
┌──────────────┐    ┌─────────────┐    ┌──────────────────────────┐    ┌──────────────┐
│  Producteur  │───▶│    Kafka    │───▶│  Spark Structured        │───▶│  PostgreSQL  │
│   (Python)   │    │ ecommerce-  │    │  Streaming (PySpark)     │    │  (métriques) │
│  événements  │    │   events    │    │  fenêtres 1 min + watermark│   │              │
└──────────────┘    └─────────────┘    └──────────────────────────┘    └──────────────┘
```

## Stack

Python · Apache Kafka · PySpark (Structured Streaming) · PostgreSQL · Docker Compose

## Événements simulés

```json
{
  "event_id": "uuid",
  "user_id": "U42",
  "product_id": "P17",
  "event_type": "view | add_to_cart | purchase",
  "price": 129.9,
  "ts": "2026-02-10T14:32:05+00:00"
}
```

Répartition simulée : 70 % de vues, 20 % d'ajouts au panier, 10 % d'achats.

## Traitements Spark

- Parsing JSON du flux Kafka, typage et horodatage (`event_time`).
- Watermark de 30 secondes pour gérer les événements en retard.
- Fenêtres temporelles d'une minute.
- Indicateurs calculés :
  - volume d'événements et chiffre d'affaires par type d'événement (`event_metrics`)
  - vues, ajouts au panier, achats et revenu par produit (`product_metrics`)
  - taux de conversion vue → achat (vue SQL `conversion_rate`)

## Lancement

```bash
docker compose up -d --build
docker compose logs -f spark
```

Les premières lignes apparaissent après 1 à 2 minutes, le temps qu'une fenêtre soit fermée par le watermark.

## Vérification des résultats

```bash
# Métriques par type d'événement
docker compose exec postgres psql -U app -d ecommerce \
  -c "SELECT * FROM event_metrics ORDER BY window_start DESC LIMIT 10;"

# Top produits par chiffre d'affaires
docker compose exec postgres psql -U app -d ecommerce \
  -c "SELECT product_id, SUM(revenue) AS ca FROM product_metrics GROUP BY product_id ORDER BY ca DESC LIMIT 5;"

# Taux de conversion
docker compose exec postgres psql -U app -d ecommerce \
  -c "SELECT * FROM conversion_rate LIMIT 10;"
```

## Arrêt

```bash
docker compose down        # conserve les données
docker compose down -v     # supprime aussi les données PostgreSQL
```

## Structure

```
kafka-spark-pipeline/
├── docker-compose.yml
├── producer/        # producteur Kafka (Python)
├── spark/           # job Spark Structured Streaming
├── sql/             # schéma PostgreSQL
└── README.md
```

## Pistes d'amélioration

- Plusieurs partitions Kafka et plusieurs consommateurs.
- Dashboard Grafana branché sur PostgreSQL.
- Écriture idempotente (upsert) pour garantir l'exactly-once côté base.
