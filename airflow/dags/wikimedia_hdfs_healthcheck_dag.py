"""
DAG 1 — Contrôle de santé HDFS (Wikimedia Big Data).

Vérifie la connectivité HDFS, les dossiers processed/reports
et produit hdfs_health_report.json pour le monitoring.
"""

from __future__ import annotations

import logging
from datetime import timedelta

import pendulum
from airflow.sdk import dag, task

from common.wikimedia_hdfs_utils import (
    HDFS_PROCESSED_PATH,
    HDFS_REPORTS_ROOT,
    REPORT_SUBDIRS,
    discover_processed_json_files,
    utc_now_iso,
    webhdfs_list_dir,
    webhdfs_path_exists,
    webhdfs_write_json,
)

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "hasso",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


@dag(
    dag_id="wikimedia_hdfs_healthcheck",
    description="Vérification HDFS : processed, rapports, connectivité WebHDFS",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["wikimedia", "hdfs", "healthcheck", "monitoring"],
    default_args=DEFAULT_ARGS,
    doc_md=__doc__,
)
def wikimedia_hdfs_healthcheck():
    @task
    def log_run_start(**context) -> None:
        """Journalise le début du run (observabilité Airflow)."""
        dag_run = context.get("dag_run")
        logger.info("=== Début DAG wikimedia_hdfs_healthcheck ===")
        logger.info("run_id = %s", dag_run.run_id if dag_run else "?")

    @task
    def check_hdfs_connectivity() -> bool:
        """Teste que WebHDFS répond sur la racine HDFS."""
        try:
            webhdfs_list_dir("/")
            logger.info("WebHDFS opérationnel (LISTSTATUS / OK).")
            return True
        except Exception as exc:
            logger.error("WebHDFS inaccessible : %s", exc)
            return False

    @task
    def inspect_processed_zone(hdfs_ok: bool) -> dict:
        """Vérifie le dossier processed et compte les fichiers JSON."""
        processed_exists = webhdfs_path_exists(HDFS_PROCESSED_PATH)
        json_files: list[str] = []

        if processed_exists:
            try:
                json_files = discover_processed_json_files()
            except Exception as exc:
                logger.warning("Erreur listing processed : %s", exc)

        result = {
            "processed_path": HDFS_PROCESSED_PATH,
            "processed_exists": processed_exists,
            "processed_json_file_count": len(json_files),
            "processed_json_files_sample": json_files[:10],
        }
        logger.info(
            "Processed : exists=%s, fichiers JSON=%d",
            processed_exists,
            len(json_files),
        )
        return result

    @task
    def inspect_reports_folders(hdfs_ok: bool) -> dict:
        """Valide l'existence des sous-dossiers de rapports."""
        folders_status = {}
        for subdir in REPORT_SUBDIRS:
            path = f"{HDFS_REPORTS_ROOT}/{subdir}"
            exists = webhdfs_path_exists(path)
            folders_status[subdir] = exists
            logger.info("Dossier %s : %s", path, "OK" if exists else "MANQUANT")

        root_exists = webhdfs_path_exists(HDFS_REPORTS_ROOT)
        return {
            "reports_root": HDFS_REPORTS_ROOT,
            "reports_root_exists": root_exists,
            "subfolders": folders_status,
        }

    @task
    def write_health_report(
        hdfs_ok: bool,
        processed_info: dict,
        reports_info: dict,
    ) -> str:
        """Assemble et écrit hdfs_health_report.json sur HDFS."""
        overall_ok = (
            hdfs_ok
            and processed_info.get("processed_exists")
            and processed_info.get("processed_json_file_count", 0) > 0
        )

        report = {
            "generated_at": utc_now_iso(),
            "dag_id": "wikimedia_hdfs_healthcheck",
            "hdfs_reachable": hdfs_ok,
            "overall_healthy": overall_ok,
            "processed": processed_info,
            "reports": reports_info,
            "message": (
                "Pipeline HDFS prêt pour les DAGs analytiques."
                if overall_ok
                else "Action requise : vérifier Spark Streaming et les dossiers HDFS."
            ),
        }

        path = f"{HDFS_REPORTS_ROOT}/hdfs_health_report.json"
        return webhdfs_write_json(path, report)

    @task
    def log_run_end(report_path: str, **context) -> None:
        logger.info("Rapport santé : %s", report_path)
        logger.info("=== Fin DAG wikimedia_hdfs_healthcheck ===")

    start = log_run_start()
    hdfs_ok = check_hdfs_connectivity()
    processed_info = inspect_processed_zone(hdfs_ok)
    reports_info = inspect_reports_folders(hdfs_ok)
    report_path = write_health_report(hdfs_ok, processed_info, reports_info)
    end = log_run_end(report_path)

    start >> hdfs_ok >> [processed_info, reports_info] >> report_path >> end


wikimedia_hdfs_healthcheck()
