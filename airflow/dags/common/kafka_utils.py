"""
Utilitaires Kafka pour les DAGs (comptage messages, lag approximatif).
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any

logger = logging.getLogger(__name__)

DEFAULT_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")

TOPICS = (
    "wm.recentchange.raw",
    "wm.bot.events",
    "wm.page.edits",
    "wm.errors",
)


def get_topic_message_counts(
    bootstrap_servers: str = DEFAULT_BOOTSTRAP,
    max_per_topic: int = 5000,
) -> dict[str, int]:
    """
    Estime le volume par topic (lecture limitée pour performance).
    """
    try:
        from kafka import KafkaConsumer
    except ImportError:
        logger.warning("kafka-python non installé")
        return {t: 0 for t in TOPICS}

    counts: dict[str, int] = {}
    for topic in TOPICS:
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=[bootstrap_servers],
                auto_offset_reset="earliest",
                enable_auto_commit=False,
                consumer_timeout_ms=3000,
                value_deserializer=lambda m: m,
            )
            n = 0
            for _ in consumer:
                n += 1
                if n >= max_per_topic:
                    break
            consumer.close()
            counts[topic] = n
        except Exception as exc:
            logger.warning("Topic %s : %s", topic, exc)
            counts[topic] = 0
    return counts


def estimate_consumer_lag(
    bootstrap_servers: str = DEFAULT_BOOTSTRAP,
    group_id: str = "wikimedia-lag-monitor",
    topic: str = "wm.recentchange.raw",
) -> dict[str, Any]:
    """
    Estime le lag consumer (différence fin de topic - offset commité).
    """
    try:
        from kafka import KafkaConsumer, TopicPartition
    except ImportError:
        return {"topic": topic, "lag": 0, "status": "kafka-python missing"}

    try:
        consumer = KafkaConsumer(
            bootstrap_servers=[bootstrap_servers],
            group_id=group_id,
            enable_auto_commit=False,
        )
        partitions = consumer.partitions_for_topic(topic) or set()
        total_lag = 0
        per_partition: dict[str, int] = {}

        for p in partitions:
            tp = TopicPartition(topic, p)
            end_offsets = consumer.end_offsets([tp])
            committed = consumer.committed(tp)
            end = end_offsets.get(tp, 0)
            current = committed if committed is not None else 0
            lag = max(0, end - current)
            per_partition[str(p)] = lag
            total_lag += lag

        consumer.close()
        return {
            "topic": topic,
            "consumer_group": group_id,
            "total_lag": total_lag,
            "per_partition": per_partition,
            "status": "ok",
        }
    except Exception as exc:
        logger.warning("Lag Kafka : %s", exc)
        return {"topic": topic, "total_lag": -1, "status": str(exc)}
