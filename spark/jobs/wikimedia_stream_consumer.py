"""
Consumer Spark Structured Streaming — Kafka → console + HDFS (Data Lake).

Pipeline ETL en continu :
  1. Lit wm.recentchange.raw (Kafka)
  2. Parse le JSON enrichi par le producteur Python
  3. Affiche les micro-lots dans la console (debug)
  4. Écrit les événements en JSON sur HDFS (zone processed)

Exécution : voir README (spark-submit + packages Kafka).
"""

from __future__ import annotations

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import (
    BooleanType,
    StringType,
    StructField,
    StructType,
)

# ---------------------------------------------------------------------------
# Kafka — adresse selon l'environnement (voir README)
# ---------------------------------------------------------------------------
# localhost:9092 : producteur sur la machine hôte
# kafka:29092    : Spark dans le réseau Docker (listener PLAINTEXT)

KAFKA_BOOTSTRAP = "kafka:29092"
KAFKA_TOPIC = "wm.recentchange.raw"

# Un groupe Kafka par requête streaming (Spark : une sink par flux).
# Console et HDFS = deux pipelines parallèles sur le même topic.
KAFKA_GROUP_CONSOLE = "wikimedia-spark-console"
KAFKA_GROUP_HDFS = "wikimedia-spark-hdfs"

# ---------------------------------------------------------------------------
# Configuration HDFS (Data Lake)
# ---------------------------------------------------------------------------
# Namenode RPC sur le port 8020 (config/hadoop.env).
HDFS_NAMENODE = "hdfs://namenode:8020"
HDFS_OUTPUT_PATH = f"{HDFS_NAMENODE}/data/wikimedia/processed"
HDFS_CHECKPOINT_PATH = f"{HDFS_NAMENODE}/data/wikimedia/checkpoints/stream-consumer"

# Schéma JSON attendu : champs ajoutés par ingestion/wikimedia_producer.py
WIKIMEDIA_RECORD_SCHEMA = StructType(
    [
        StructField("title", StringType(), nullable=True),
        StructField("wiki", StringType(), nullable=True),
        StructField("user", StringType(), nullable=True),
        StructField("is_bot", BooleanType(), nullable=True),
        StructField("is_anonymous", BooleanType(), nullable=True),
        StructField("event_type", StringType(), nullable=True),
        StructField("ingestion_timestamp", StringType(), nullable=True),
        StructField("source", StringType(), nullable=True),
        StructField("partition_key", StringType(), nullable=True),
        StructField("page_id", StringType(), nullable=True),
    ]
)


def create_spark_session() -> SparkSession:
    """
    Crée une SparkSession avec accès HDFS.

    fs.defaultFS indique à Spark où écrire les chemins hdfs://namenode:8020/...
    """
    return (
        SparkSession.builder.appName("WikimediaKafkaStreamConsumer")
        .config("spark.hadoop.fs.defaultFS", HDFS_NAMENODE)
        .getOrCreate()
    )


def read_kafka_stream(
    spark: SparkSession,
    bootstrap_servers: str,
    topic: str,
    consumer_group: str,
):
    """
    On lit les événements depuis le topic Kafka (source streaming).

    consumer_group : Kafka mémorise les offsets par groupe (reprise après arrêt).
    """
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("kafka.group.id", consumer_group)
        .option("startingOffsets", "earliest")
        .load()
    )


def parse_kafka_values(kafka_df):
    """
    On convertit la valeur Kafka (bytes) en texte JSON, puis on parse avec from_json.
    """
    json_strings = kafka_df.select(
        col("key").cast("string").alias("kafka_key"),
        col("value").cast("string").alias("json_value"),
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp"),
    )

    parsed = json_strings.select(
        col("kafka_key"),
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp"),
        from_json(col("json_value"), WIKIMEDIA_RECORD_SCHEMA).alias("event"),
    )

    return parsed.select(
        col("kafka_key"),
        col("topic").alias("kafka_topic"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("timestamp").alias("kafka_timestamp"),
        col("event.title"),
        col("event.wiki"),
        col("event.user"),
        col("event.is_bot"),
        col("event.is_anonymous"),
        col("event.event_type"),
        col("event.ingestion_timestamp"),
        col("event.source"),
        col("event.partition_key"),
        col("event.page_id"),
    )


def add_event_time_column(events_df):
    """
    Colonne event_time (timestamp Spark) dérivée de ingestion_timestamp.
    Utile pour le tri et les analyses temporelles sur HDFS.
    """
    return events_df.withColumn(
        "event_time",
        to_timestamp(col("ingestion_timestamp")),
    )


def build_events_pipeline(
    spark: SparkSession,
    bootstrap_servers: str,
    consumer_group: str,
):
    """Enchaîne Kafka → parse JSON → event_time pour une requête streaming donnée."""
    kafka_stream = read_kafka_stream(
        spark, bootstrap_servers, KAFKA_TOPIC, consumer_group
    )
    return add_event_time_column(parse_kafka_values(kafka_stream))


def start_console_sink(events_df):
    """Affiche chaque micro-lot dans la console (vérification rapide)."""
    return (
        events_df.writeStream.outputMode("append")
        .format("console")
        .option("truncate", False)
        .queryName("wikimedia_console_debug")
        .start()
    )


def start_hdfs_json_sink(events_df):
    """
    Écrit les événements en JSON sur HDFS (une ligne = un enregistrement).

    checkpointLocation : permet à Spark de reprendre le traitement après un arrêt.
    """
    return (
        events_df.writeStream.outputMode("append")
        .format("json")
        .option("path", HDFS_OUTPUT_PATH)
        .option("checkpointLocation", HDFS_CHECKPOINT_PATH)
        .queryName("wikimedia_hdfs_json")
        .start()
    )


def main() -> None:
    bootstrap = sys.argv[1] if len(sys.argv) > 1 else KAFKA_BOOTSTRAP

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("=" * 60)
    print(" Wikimedia — Streaming ETL (Kafka → console + HDFS)")
    print("=" * 60)
    print(f"[INFO] Kafka bootstrap      : {bootstrap}")
    print(f"[INFO] Topic                : {KAFKA_TOPIC}")
    print(f"[INFO] HDFS sortie (JSON)   : {HDFS_OUTPUT_PATH}")
    print(f"[INFO] HDFS checkpoint      : {HDFS_CHECKPOINT_PATH}")
    print("[INFO] Lancez le producteur si le topic est vide.\n")

    # Deux requêtes en parallèle : même transformations, groupes Kafka différents
    events_console = build_events_pipeline(spark, bootstrap, KAFKA_GROUP_CONSOLE)
    events_hdfs = build_events_pipeline(spark, bootstrap, KAFKA_GROUP_HDFS)

    print("[INFO] Schéma du flux (avec event_time) :")
    events_hdfs.printSchema()

    console_query = start_console_sink(events_console)
    hdfs_query = start_hdfs_json_sink(events_hdfs)

    print("[OK]   Requête console démarrée (debug).")
    print("[OK]   Requête HDFS JSON démarrée (Data Lake).\n")

    try:
        # Boucle jusqu'à CTRL+C
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("\n[INFO] Arrêt demandé. Fermeture des requêtes streaming…")
        console_query.stop()
        hdfs_query.stop()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
