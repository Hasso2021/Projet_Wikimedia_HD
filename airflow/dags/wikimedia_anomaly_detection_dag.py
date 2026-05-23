"""
DAG 4 — Détection d'anomalies (DATA QUALITY + OPS).

Sorties : hdfs://data/wikimedia/anomalies/
"""

from __future__ import annotations

import logging
from datetime import timedelta

import pendulum
from airflow.sdk import dag, task

from common.anomaly_utils import detect_anomalies
from common.wikimedia_hdfs_utils import (
    HDFS_ANOMALIES_ROOT,
    load_all_processed_events,
    utc_now_iso,
    webhdfs_write_json,
)

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "hasso",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


@dag(
    dag_id="wikimedia_anomaly_detection",
    description="Détection anomalies : spike, bots, spam, qualité données",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="0 */1 * * *",
    catchup=False,
    tags=["wikimedia", "anomaly", "data-quality", "hdfs"],
    default_args=DEFAULT_ARGS,
    doc_md=__doc__,
)
def wikimedia_anomaly_detection():
    @task
    def log_run_start() -> None:
        logger.info("=== Début DAG wikimedia_anomaly_detection ===")

    @task
    def load_and_detect() -> dict:
        events, files = load_all_processed_events()
        if not events:
            raise ValueError(
                "Aucun événement processed — lancez ingestion + Spark Streaming."
            )
        anomalies = detect_anomalies(events)
        logger.info("%d anomalie(s) détectée(s) sur %d événements.", len(anomalies), len(events))
        return {
            "events_analyzed": len(events),
            "files_read": len(files),
            "anomalies": anomalies,
            "anomaly_count": len(anomalies),
        }

    @task
    def save_anomalies(payload: dict) -> str:
        generated = utc_now_iso()
        body = {
            "generated_at": generated,
            "events_analyzed": payload["events_analyzed"],
            "anomaly_count": payload["anomaly_count"],
            "anomalies": payload["anomalies"],
        }
        summary_path = f"{HDFS_ANOMALIES_ROOT}/anomalies_summary.json"
        webhdfs_write_json(summary_path, body)

        run_path = f"{HDFS_ANOMALIES_ROOT}/run_{generated.replace(':', '-').replace('+', '')}.json"
        webhdfs_write_json(run_path, body)
        return summary_path

    @task
    def log_run_end(path: str) -> None:
        logger.info("Anomalies écrites : %s", path)
        logger.info("=== Fin DAG wikimedia_anomaly_detection ===")

    start = log_run_start()
    detected = load_and_detect()
    path = save_anomalies(detected)
    end = log_run_end(path)
    start >> detected >> path >> end


wikimedia_anomaly_detection()
