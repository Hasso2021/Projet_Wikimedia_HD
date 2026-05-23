"""
DAG 6 — Reporting automatisé (activité, qualité, trafic, système).

Sortie principale : hdfs://data/wikimedia/reports/YYYY-MM-DD.json
"""

from __future__ import annotations

import logging
from collections import Counter
from datetime import timedelta

import pendulum
from airflow.sdk import dag, task

from common.kafka_utils import estimate_consumer_lag, get_topic_message_counts
from common.wikimedia_hdfs_utils import (
    HDFS_REPORTS_ROOT,
    load_all_anomalies,
    load_all_processed_events,
    today_report_date,
    top_n_counter,
    utc_now_iso,
    webhdfs_write_json,
    wiki_to_language,
    page_key,
    event_timestamp,
)

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "hasso",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


@dag(
    dag_id="wikimedia_automated_reporting",
    description="Rapports journaliers : activité, qualité, trafic, système",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="30 */2 * * *",
    catchup=False,
    tags=["wikimedia", "reporting", "hdfs"],
    default_args=DEFAULT_ARGS,
    doc_md=__doc__,
)
def wikimedia_automated_reporting():
    @task
    def log_run_start() -> None:
        logger.info("=== Début DAG wikimedia_automated_reporting ===")

    @task
    def build_daily_report() -> dict:
        events, files = load_all_processed_events()
        anomalies = load_all_anomalies()
        topic_counts = get_topic_message_counts()
        kafka_lag = estimate_consumer_lag()

        total = len(events)
        bots = sum(1 for e in events if e.get("is_bot") is True)
        humans = total - bots
        by_hour: Counter = Counter()
        by_lang: Counter = Counter()
        by_wiki: Counter = Counter()
        user_counter: Counter = Counter()
        page_counter: Counter = Counter()

        for event in events:
            by_wiki[str(event.get("wiki") or "unknown")] += 1
            by_lang[wiki_to_language(event.get("wiki"))] += 1
            user_counter[str(event.get("user") or "unknown")] += 1
            page_counter[page_key(event)] += 1
            dt = event_timestamp(event)
            if dt:
                by_hour[dt.strftime("%Y-%m-%d %H:00")] += 1

        errors_count = topic_counts.get("wm.errors", 0)
        invalid_rate = round(
            (errors_count / max(1, errors_count + total)) * 100, 2
        )

        report_date = today_report_date()
        generated = utc_now_iso()

        global_report = {
            "generated_at": generated,
            "report_date": report_date,
            "total_events": total,
            "files_read": len(files),
            "top_pages": [
                {
                    "rank": rank,
                    "page": f"{title} ({wiki})",
                    "edit_count": count,
                }
                for rank, ((title, wiki), count) in enumerate(
                    page_counter.most_common(10), start=1
                )
            ],
            "top_users": top_n_counter(user_counter, 10, "user", "edit_count"),
            "bot_human_ratio": {
                "bots": bots,
                "humans": humans,
                "bot_percentage": round((bots / total) * 100, 2) if total else 0,
            },
        }

        quality_report = {
            "generated_at": generated,
            "anomalies_detected": len(anomalies),
            "anomalies_sample": anomalies[:20],
            "ingestion_errors_topic_count": errors_count,
            "invalid_events_rate_pct": invalid_rate,
        }

        traffic_report = {
            "generated_at": generated,
            "by_hour": dict(by_hour.most_common(48)),
            "by_language": dict(by_lang.most_common()),
            "by_wiki": dict(by_wiki.most_common()),
            "kafka_topic_volumes": topic_counts,
        }

        system_report = {
            "generated_at": generated,
            "kafka_lag": kafka_lag,
            "spark_note": "Voir UI http://spark-master:8080 pour jobs actifs",
            "hdfs_reports_root": HDFS_REPORTS_ROOT,
        }

        daily = {
            "generated_at": generated,
            "report_date": report_date,
            "global_activity_report": global_report,
            "quality_report": quality_report,
            "traffic_report": traffic_report,
            "system_report": system_report,
        }
        return daily

    @task
    def save_daily_report(daily: dict) -> str:
        report_date = daily["report_date"]
        main_path = f"{HDFS_REPORTS_ROOT}/{report_date}.json"
        webhdfs_write_json(main_path, daily)

        webhdfs_write_json(
            f"{HDFS_REPORTS_ROOT}/quality/quality_report_{report_date}.json",
            daily["quality_report"],
        )
        webhdfs_write_json(
            f"{HDFS_REPORTS_ROOT}/traffic/traffic_report_{report_date}.json",
            daily["traffic_report"],
        )
        webhdfs_write_json(
            f"{HDFS_REPORTS_ROOT}/system/system_report_{report_date}.json",
            daily["system_report"],
        )
        return main_path

    @task
    def log_run_end(path: str) -> None:
        logger.info("Rapport journalier : %s", path)
        logger.info("=== Fin DAG wikimedia_automated_reporting ===")

    start = log_run_start()
    daily = build_daily_report()
    path = save_daily_report(daily)
    end = log_run_end(path)
    start >> daily >> path >> end


wikimedia_automated_reporting()
