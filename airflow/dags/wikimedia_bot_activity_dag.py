"""
DAG 4 — Analytique bots (ratio, bots les plus actifs).
"""

from __future__ import annotations

import logging
from collections import Counter
from datetime import timedelta

import pendulum
from airflow.sdk import dag, task

from common.wikimedia_hdfs_utils import (
    HDFS_REPORTS_ROOT,
    load_all_processed_events,
    top_n_counter,
    utc_now_iso,
    webhdfs_write_json,
)

logger = logging.getLogger(__name__)
REPORTS_DIR = f"{HDFS_REPORTS_ROOT}/bots"
TOP_BOTS = 20

DEFAULT_ARGS = {
    "owner": "hasso",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


@dag(
    dag_id="wikimedia_bot_activity",
    description="Rapports bots : volume, ratio bot/humain, bots actifs",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["wikimedia", "analytics", "bots", "hdfs"],
    default_args=DEFAULT_ARGS,
    doc_md=__doc__,
)
def wikimedia_bot_activity():
    @task
    def log_run_start(**context) -> None:
        logger.info("=== Début DAG wikimedia_bot_activity ===")

    @task
    def load_events() -> dict:
        events, files = load_all_processed_events()
        if not events:
            raise ValueError("Aucun événement processed.")
        return {"events": events, "total_events": len(events)}

    @task
    def compute_bot_analytics(payload: dict) -> dict:
        events = payload["events"]
        bot_users: Counter = Counter()
        bots = 0
        humans = 0

        for event in events:
            if event.get("is_bot") is True:
                bots += 1
                bot_users[str(event.get("user") or "unknown_bot")] += 1
            else:
                humans += 1

        total = payload["total_events"]
        return {
            "generated_at": utc_now_iso(),
            "total_events": total,
            "bot_volume": bots,
            "human_volume": humans,
            "bot_ratio": {
                "bots": bots,
                "humans": humans,
                "bot_percentage": round((bots / total) * 100, 2) if total else 0.0,
            },
            "active_bots": top_n_counter(bot_users, TOP_BOTS, "bot_user", "edit_count"),
        }

    @task
    def save_reports(analytics: dict) -> dict[str, str]:
        return {
            "bot_ratio.json": webhdfs_write_json(
                f"{REPORTS_DIR}/bot_ratio.json",
                {
                    "generated_at": analytics["generated_at"],
                    "total_events": analytics["total_events"],
                    "bot_volume": analytics["bot_volume"],
                    **analytics["bot_ratio"],
                },
            ),
            "active_bots.json": webhdfs_write_json(
                f"{REPORTS_DIR}/active_bots.json",
                {
                    "generated_at": analytics["generated_at"],
                    "active_bots": analytics["active_bots"],
                },
            ),
        }

    @task
    def log_run_end(paths: dict) -> None:
        logger.info("Rapports bots : %s", paths)
        logger.info("=== Fin DAG wikimedia_bot_activity ===")

    start = log_run_start()
    data = load_events()
    analytics = compute_bot_analytics(data)
    paths = save_reports(analytics)
    end = log_run_end(paths)

    start >> data >> analytics >> paths >> end


wikimedia_bot_activity()
