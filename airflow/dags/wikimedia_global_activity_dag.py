"""
DAG 5 — Analytique activité globale (minute, heure, langue, wiki).
"""

from __future__ import annotations

import logging
from collections import Counter
from datetime import timedelta

import pendulum
from airflow.sdk import dag, task

from common.wikimedia_hdfs_utils import (
    HDFS_PROCESSED_PATH,
    HDFS_REPORTS_ROOT,
    event_timestamp,
    load_all_processed_events,
    wiki_to_language,
    utc_now_iso,
    webhdfs_write_json,
)

logger = logging.getLogger(__name__)
REPORTS_DIR = f"{HDFS_REPORTS_ROOT}/global"

DEFAULT_ARGS = {
    "owner": "hasso",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


@dag(
    dag_id="wikimedia_global_activity",
    description="Rapports globaux : par minute, heure, langue, wiki",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["wikimedia", "analytics", "global", "hdfs"],
    default_args=DEFAULT_ARGS,
    doc_md=__doc__,
)
def wikimedia_global_activity():
    @task
    def log_run_start(**context) -> None:
        logger.info("=== Début DAG wikimedia_global_activity ===")

    @task
    def load_events() -> dict:
        events, files = load_all_processed_events()
        if not events:
            raise ValueError("Aucun événement processed.")
        return {"events": events, "files_read": len(files), "total_events": len(events)}

    @task
    def compute_global_analytics(payload: dict) -> dict:
        events = payload["events"]
        by_minute: Counter = Counter()
        by_hour: Counter = Counter()
        by_language: Counter = Counter()
        by_wiki: Counter = Counter()

        for event in events:
            by_wiki[str(event.get("wiki") or "unknown")] += 1
            by_language[wiki_to_language(event.get("wiki"))] += 1

            dt = event_timestamp(event)
            if dt:
                by_minute[dt.strftime("%Y-%m-%d %H:%M")] += 1
                by_hour[dt.strftime("%Y-%m-%d %H:00")] += 1
            else:
                by_minute["unknown"] += 1
                by_hour["unknown"] += 1

        return {
            "generated_at": utc_now_iso(),
            "total_events": payload["total_events"],
            "files_read": payload["files_read"],
            "source_path": HDFS_PROCESSED_PATH,
            "activity_by_minute": dict(by_minute.most_common()),
            "activity_by_hour": dict(by_hour.most_common()),
            "language_distribution": dict(by_language.most_common()),
            "activity_by_wiki": dict(by_wiki.most_common()),
        }

    @task
    def save_reports(analytics: dict) -> dict[str, str]:
        generated = analytics["generated_at"]
        paths = {}

        reports_map = {
            "activity_by_minute.json": {
                "generated_at": generated,
                "description": "Nombre d'événements par minute (UTC)",
                "activity_by_minute": analytics["activity_by_minute"],
            },
            "activity_by_hour.json": {
                "generated_at": generated,
                "description": "Nombre d'événements par heure (UTC)",
                "activity_by_hour": analytics["activity_by_hour"],
            },
            "language_distribution.json": {
                "generated_at": generated,
                "description": "Répartition par langue déduite du code wiki",
                "language_distribution": analytics["language_distribution"],
            },
            "activity_by_wiki.json": {
                "generated_at": generated,
                "description": "Répartition par wiki (enwiki, frwiki, …)",
                "events_by_wiki": analytics["activity_by_wiki"],
            },
            "global_activity.json": {
                "generated_at": generated,
                "total_events": analytics["total_events"],
                "files_read": analytics["files_read"],
                "source_path": analytics["source_path"],
            },
        }

        for filename, body in reports_map.items():
            paths[filename] = webhdfs_write_json(f"{REPORTS_DIR}/{filename}", body)

        return paths

    @task
    def log_run_end(paths: dict) -> None:
        logger.info("Rapports global : %s", paths)
        logger.info("=== Fin DAG wikimedia_global_activity ===")

    start = log_run_start()
    data = load_events()
    analytics = compute_global_analytics(data)
    paths = save_reports(analytics)
    end = log_run_end(paths)

    start >> data >> analytics >> paths >> end


wikimedia_global_activity()
