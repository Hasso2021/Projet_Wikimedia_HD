"""
DAG 3 — Analytique utilisateurs (contributeurs, anonymes, bots vs humains).
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
REPORTS_DIR = f"{HDFS_REPORTS_ROOT}/users"
TOP_USERS = 20

DEFAULT_ARGS = {
    "owner": "hasso",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


@dag(
    dag_id="wikimedia_user_activity",
    description="Rapports utilisateurs : top contributeurs, anonymes vs connectés",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["wikimedia", "analytics", "users", "hdfs"],
    default_args=DEFAULT_ARGS,
    doc_md=__doc__,
)
def wikimedia_user_activity():
    @task
    def log_run_start(**context) -> None:
        logger.info("=== Début DAG wikimedia_user_activity ===")

    @task
    def load_events() -> dict:
        events, files = load_all_processed_events()
        if not events:
            raise ValueError("Aucun événement processed — lancez ingestion + Spark.")
        return {"events": events, "files_read": len(files), "total_events": len(events)}

    @task
    def compute_user_analytics(payload: dict) -> dict:
        events = payload["events"]
        user_counter: Counter = Counter()
        human_users: Counter = Counter()
        anonymous_count = 0
        logged_count = 0
        bots = 0
        humans = 0

        for event in events:
            user = str(event.get("user") or "unknown")
            is_bot = event.get("is_bot") is True
            is_anon = event.get("is_anonymous") is True

            user_counter[user] += 1
            if is_bot:
                bots += 1
            else:
                humans += 1
                human_users[user] += 1

            if is_anon:
                anonymous_count += 1
            else:
                logged_count += 1

        total = len(events)
        bot_pct = round((bots / total) * 100, 2) if total else 0.0

        return {
            "generated_at": utc_now_iso(),
            "total_events": total,
            "user_activity": {
                "top_contributors": top_n_counter(
                    human_users, TOP_USERS, "user", "edit_count"
                ),
                "bots_vs_humans": {
                    "bots": bots,
                    "humans": humans,
                    "bot_percentage": bot_pct,
                },
            },
            "anonymous_vs_logged": {
                "anonymous": anonymous_count,
                "logged_in": logged_count,
                "anonymous_percentage": round((anonymous_count / total) * 100, 2)
                if total
                else 0.0,
                "logged_in_percentage": round((logged_count / total) * 100, 2)
                if total
                else 0.0,
            },
        }

    @task
    def save_reports(analytics: dict) -> dict[str, str]:
        paths = {}
        paths["user_activity.json"] = webhdfs_write_json(
            f"{REPORTS_DIR}/user_activity.json",
            {
                "generated_at": analytics["generated_at"],
                "total_events": analytics["total_events"],
                **analytics["user_activity"],
            },
        )
        paths["anonymous_vs_logged.json"] = webhdfs_write_json(
            f"{REPORTS_DIR}/anonymous_vs_logged.json",
            {
                "generated_at": analytics["generated_at"],
                "total_events": analytics["total_events"],
                **analytics["anonymous_vs_logged"],
            },
        )
        return paths

    @task
    def log_run_end(paths: dict) -> None:
        logger.info("Rapports users : %s", paths)
        logger.info("=== Fin DAG wikimedia_user_activity ===")

    start = log_run_start()
    data = load_events()
    analytics = compute_user_analytics(data)
    paths = save_reports(analytics)
    end = log_run_end(paths)

    start >> data >> analytics >> paths >> end


wikimedia_user_activity()
