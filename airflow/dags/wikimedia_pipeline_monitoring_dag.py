"""
DAG 7 — Monitoring Airflow + pipelines.

Collecte : statuts DAG, durées tâches, retries, taux d'échec.
"""

from __future__ import annotations

import logging
from datetime import timedelta

import pendulum
from airflow.sdk import dag, task

from common.airflow_api_utils import collect_dag_monitoring_snapshot
from common.kafka_utils import estimate_consumer_lag, get_topic_message_counts
from common.wikimedia_hdfs_utils import (
    HDFS_REPORTS_ROOT,
    today_report_date,
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
    dag_id="wikimedia_pipeline_monitoring",
    description="Monitoring : DAG status, task duration, retries, failure rate",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="*/15 * * * *",
    catchup=False,
    tags=["wikimedia", "monitoring", "airflow", "ops"],
    default_args=DEFAULT_ARGS,
    doc_md=__doc__,
)
def wikimedia_pipeline_monitoring():
    @task
    def log_run_start() -> None:
        logger.info("=== Début DAG wikimedia_pipeline_monitoring ===")

    @task
    def collect_monitoring_data() -> dict:
        airflow_snapshot = collect_dag_monitoring_snapshot()
        kafka_lag = estimate_consumer_lag()
        topic_volumes = get_topic_message_counts()

        failed_dags = sum(
            1
            for d in airflow_snapshot.get("dags", [])
            if d.get("last_run_state") == "failed"
        )
        total_dags = len(airflow_snapshot.get("dags", []))
        dag_failure_rate = round(
            (failed_dags / max(1, total_dags)) * 100, 2
        )

        return {
            "generated_at": utc_now_iso(),
            "report_date": today_report_date(),
            "airflow": airflow_snapshot,
            "kafka": {
                "lag": kafka_lag,
                "topic_volumes": topic_volumes,
            },
            "pipeline_metrics": {
                "dag_failure_rate_pct": dag_failure_rate,
                "ingestion_kafka_lag": kafka_lag.get("total_lag"),
                "spark_processing_note": "Latence Spark : voir batch_duration dans UI Master",
            },
        }

    @task
    def save_monitoring_report(payload: dict) -> str:
        report_date = payload["report_date"]
        path = f"{HDFS_REPORTS_ROOT}/monitoring/pipeline_monitoring_{report_date}.json"
        webhdfs_write_json(path, payload)
        webhdfs_write_json(
            f"{HDFS_REPORTS_ROOT}/monitoring/latest_pipeline_monitoring.json",
            payload,
        )
        return path

    @task
    def log_run_end(path: str) -> None:
        logger.info("Monitoring écrit : %s", path)
        logger.info("=== Fin DAG wikimedia_pipeline_monitoring ===")

    start = log_run_start()
    data = collect_monitoring_data()
    path = save_monitoring_report(data)
    end = log_run_end(path)
    start >> data >> path >> end


wikimedia_pipeline_monitoring()
