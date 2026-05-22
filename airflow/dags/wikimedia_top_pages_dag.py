"""
DAG 2 — Analytique « Top pages » (pages modifiées, créées, supprimées).
"""

from __future__ import annotations

import logging
from collections import Counter
from datetime import timedelta

import pendulum
from airflow.sdk import dag, task

from common.wikimedia_hdfs_utils import (
    CREATE_TYPES,
    DELETE_TYPES,
    HDFS_REPORTS_ROOT,
    load_all_processed_events,
    page_key,
    utc_now_iso,
    webhdfs_write_json,
)

logger = logging.getLogger(__name__)
TOP_LIMIT = 20
REPORTS_DIR = f"{HDFS_REPORTS_ROOT}/top_pages"

DEFAULT_ARGS = {
    "owner": "hasso",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


@dag(
    dag_id="wikimedia_top_pages",
    description="Rapports top pages : modifiées, créées, supprimées",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["wikimedia", "analytics", "pages", "hdfs"],
    default_args=DEFAULT_ARGS,
    doc_md=__doc__,
)
def wikimedia_top_pages():
    @task
    def log_run_start(**context) -> None:
        logger.info("=== Début DAG wikimedia_top_pages === run_id=%s", context.get("dag_run"))

    @task
    def load_events() -> dict:
        """Charge tous les événements processed depuis HDFS."""
        events, files = load_all_processed_events()
        if not events:
            raise ValueError(
                "Aucun événement dans processed/. Lancez le producteur et Spark Streaming."
            )
        logger.info("%d événements chargés depuis %d fichier(s).", len(events), len(files))
        return {"events": events, "files_read": len(files), "total_events": len(events)}

    @task
    def compute_page_analytics(payload: dict) -> dict:
        """
        Calcule :
          - pages les plus modifiées (tous types confondus)
          - pages les plus créées (event_type new/create)
          - pages les plus supprimées (event_type delete/remove)
        """
        events = payload["events"]
        all_pages: Counter = Counter()
        created_pages: Counter = Counter()
        deleted_pages: Counter = Counter()

        for event in events:
            key = page_key(event)
            event_type = str(event.get("event_type", "")).lower()
            all_pages[key] += 1
            if event_type in CREATE_TYPES:
                created_pages[key] += 1
            if event_type in DELETE_TYPES:
                deleted_pages[key] += 1

        def format_page_ranking(counter: Counter) -> list[dict]:
            ranked = counter.most_common(TOP_LIMIT)
            return [
                {
                    "rank": i,
                    "title": title,
                    "wiki": wiki,
                    "edit_count": count,
                }
                for i, ((title, wiki), count) in enumerate(ranked, start=1)
            ]

        return {
            "generated_at": utc_now_iso(),
            "total_events": payload["total_events"],
            "files_read": payload["files_read"],
            "top_pages": format_page_ranking(all_pages),
            "most_created_pages": format_page_ranking(created_pages),
            "most_deleted_pages": format_page_ranking(deleted_pages),
        }

    @task
    def save_reports(analytics: dict) -> dict[str, str]:
        """Écrit les 3 JSON dans /reports/top_pages/."""
        paths = {}
        mapping = {
            "top_pages.json": {
                "generated_at": analytics["generated_at"],
                "description": "Pages les plus modifiées (tous types d'événements)",
                "total_events": analytics["total_events"],
                "top_pages": analytics["top_pages"],
            },
            "most_created_pages.json": {
                "generated_at": analytics["generated_at"],
                "description": "Pages les plus créées (event_type new/create)",
                "most_created_pages": analytics["most_created_pages"],
            },
            "most_deleted_pages.json": {
                "generated_at": analytics["generated_at"],
                "description": "Pages les plus supprimées (event_type delete/remove)",
                "most_deleted_pages": analytics["most_deleted_pages"],
            },
        }
        for filename, body in mapping.items():
            hdfs_path = f"{REPORTS_DIR}/{filename}"
            paths[filename] = webhdfs_write_json(hdfs_path, body)
        return paths

    @task
    def log_run_end(paths: dict) -> None:
        for name, path in paths.items():
            logger.info("Rapport %s → %s", name, path)
        logger.info("=== Fin DAG wikimedia_top_pages ===")

    start = log_run_start()
    data = load_events()
    analytics = compute_page_analytics(data)
    paths = save_reports(analytics)
    end = log_run_end(paths)

    start >> data >> analytics >> paths >> end


wikimedia_top_pages()
