from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, from_json, sum as _sum, to_timestamp, when, window,
)
from pyspark.sql.types import DoubleType, StringType, StructType

KAFKA_SERVERS = "kafka:9092"
TOPIC = "ecommerce-events"
JDBC_URL = "jdbc:postgresql://postgres:5432/ecommerce"
JDBC_PROPS = {"user": "app", "password": "app", "driver": "org.postgresql.Driver"}

spark = SparkSession.builder.appName("EcommerceStreaming").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

schema = (
    StructType()
    .add("event_id", StringType())
    .add("user_id", StringType())
    .add("product_id", StringType())
    .add("event_type", StringType())
    .add("price", DoubleType())
    .add("ts", StringType())
)

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_SERVERS)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

events = (
    raw.select(from_json(col("value").cast("string"), schema).alias("e"))
    .select("e.*")
    .withColumn("event_time", to_timestamp("ts"))
    .dropna(subset=["event_time", "event_type"])
    .withWatermark("event_time", "30 seconds")
)

# ---- Indicateur 1 : volume et chiffre d'affaires par type d'événement ----
event_metrics = (
    events.groupBy(window("event_time", "1 minute"), "event_type")
    .agg(
        count("*").alias("event_count"),
        _sum(when(col("event_type") == "purchase", col("price")).otherwise(0.0)).alias("revenue"),
    )
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "event_type",
        "event_count",
        "revenue",
    )
)

# ---- Indicateur 2 : activité par produit ----
product_metrics = (
    events.groupBy(window("event_time", "1 minute"), "product_id")
    .agg(
        _sum(when(col("event_type") == "view", 1).otherwise(0)).alias("views"),
        _sum(when(col("event_type") == "add_to_cart", 1).otherwise(0)).alias("carts"),
        _sum(when(col("event_type") == "purchase", 1).otherwise(0)).alias("purchases"),
        _sum(when(col("event_type") == "purchase", col("price")).otherwise(0.0)).alias("revenue"),
    )
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "product_id",
        "views",
        "carts",
        "purchases",
        "revenue",
    )
)


def writer_for(table):
    def write_batch(df, batch_id):
        if df.rdd.isEmpty():
            return
        df.write.jdbc(url=JDBC_URL, table=table, mode="append", properties=JDBC_PROPS)
    return write_batch


q1 = (
    event_metrics.writeStream.outputMode("append")
    .foreachBatch(writer_for("event_metrics"))
    .option("checkpointLocation", "/tmp/checkpoints/event_metrics")
    .start()
)
q2 = (
    product_metrics.writeStream.outputMode("append")
    .foreachBatch(writer_for("product_metrics"))
    .option("checkpointLocation", "/tmp/checkpoints/product_metrics")
    .start()
)

spark.streams.awaitAnyTermination()
