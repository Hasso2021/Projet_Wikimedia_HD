"""
DAG 1 — Ingestion Wikimedia → Kafka.

Exécute le producteur Python par lots planifiés (toutes les 5 minutes).
Les événements invalides sont journalisés et envoyés vers wm.errors.
"""

from __future__ import annotations

import logging
import os
import subprocess
import sys
from datetime import timedelta

import pendulum
from airflow.sdk import dag, task

logger = logging.getLogger(__name__)

INGESTION_SCRIPT = "/opt/airflow/ingestion/wikimedia_producer.py"
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
BATCH_SIZE = os.getenv("INGESTION_BATCH_SIZE", "300")

DEFAULT_ARGS = {
    "owner": "hasso",
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
}


@dag(
    dag_id="wikimedia_ingestion",
    description="Ingestion Wikimedia SSE → Kafka (raw, bot, page.edits, errors)",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="*/5 * * * *",
    catchup=False,
    tags=["wikimedia", "ingestion", "kafka"],
    default_args=DEFAULT_ARGS,
    doc_md=__doc__,
)
# 
def wikimedia_ingestion():
    @task
    def log_run_start(**context) -> None:
        dag_run = context.get("dag_run")
        logger.info("=== Début DAG wikimedia_ingestion ===")
        logger.info("run_id = %s", dag_run.run_id if dag_run else "?")

    @task
    def run_ingestion_batch() -> dict:
        """
        Lance le producteur Python (batch de N événements).
        Retry automatique géré par default_args du DAG.
        """
        cmd = [
            sys.executable,
            INGESTION_SCRIPT,
            "--max-events",
            BATCH_SIZE,
            "--kafka-bootstrap",
            KAFKA_BOOTSTRAP,
        ]
        logger.info("Commande : %s", " ".join(cmd))
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=600,
            check=False,
        )
        if result.stdout:
            logger.info(result.stdout[-4000:])
        if result.returncode != 0:
            logger.error(result.stderr[-2000:] if result.stderr else "échec ingestion")
            raise RuntimeError(
                f"Producteur terminé avec code {result.returncode}: {result.stderr[:500]}"
            )
        return {
            "status": "success",
            "batch_size": BATCH_SIZE,
            "kafka_bootstrap": KAFKA_BOOTSTRAP,
        }

    @task
    def log_rejected_events_summary() -> None:
        """
        Rappel : les événements rejetés sont dans wm.errors (voir logs producteur).
        """
        logger.info(
            "Événements invalides → topic wm.errors | "
            "Bots → wm.bot.events | Éditions → wm.page.edits | "
            "Tous valides → wm.recentchange.raw"
        )
        logger.info("=== Fin DAG wikimedia_ingestion ===")

    start = log_run_start()
    batch = run_ingestion_batch()
    end = log_rejected_events_summary()
    start >> batch >> end


wikimedia_ingestion()
