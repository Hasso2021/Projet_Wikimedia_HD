"""
Appels API Airflow 3 pour le DAG de monitoring pipelines.
"""

from __future__ import annotations

import json
import logging
import os
import urllib.error
import urllib.request
from typing import Any

logger = logging.getLogger(__name__)

AIRFLOW_API_BASE = os.getenv(
    "AIRFLOW_API_BASE",
    "http://airflow-api-server:8080/api/v2",
).rstrip("/")

AIRFLOW_ADMIN_USER = os.getenv("AIRFLOW_ADMIN_USER", "admin")
AIRFLOW_ADMIN_PASSWORD = os.getenv("AIRFLOW_ADMIN_PASSWORD", "admin")

_JWT_CACHE: str | None = None


def _airflow_root_url() -> str:
    if AIRFLOW_API_BASE.endswith("/api/v2"):
        return AIRFLOW_API_BASE[: -len("/api/v2")]
    return "http://airflow-api-server:8080"


def _get_jwt_token() -> str:
    """JWT requis pour l'API publique Airflow 3 (/api/v2)."""
    global _JWT_CACHE
    if _JWT_CACHE:
        return _JWT_CACHE

    token_url = f"{_airflow_root_url()}/auth/token"
    body = json.dumps(
        {"username": AIRFLOW_ADMIN_USER, "password": AIRFLOW_ADMIN_PASSWORD}
    ).encode("utf-8")
    req = urllib.request.Request(
        token_url,
        data=body,
        method="POST",
        headers={"Content-Type": "application/json", "Accept": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=15) as resp:
        payload = json.loads(resp.read().decode("utf-8"))
    token = payload.get("access_token")
    if not token:
        raise RuntimeError(f"Pas de access_token dans la réponse : {payload}")
    _JWT_CACHE = token
    return token


def _api_get(path: str, timeout: int = 15) -> dict[str, Any] | list[Any]:
    url = f"{AIRFLOW_API_BASE}{path}"
    token = _get_jwt_token()
    req = urllib.request.Request(
        url,
        headers={
            "Accept": "application/json",
            "Authorization": f"Bearer {token}",
        },
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


WIKIMEDIA_DAG_IDS = (
    "wikimedia_ingestion",
    "wikimedia_hdfs_healthcheck",
    "wikimedia_global_activity",
    "wikimedia_top_pages",
    "wikimedia_user_activity",
    "wikimedia_bot_activity",
    "wikimedia_anomaly_detection",
    "wikimedia_automated_reporting",
    "wikimedia_pipeline_monitoring",
    "hello_world",
)


def collect_dag_monitoring_snapshot() -> dict[str, Any]:
    """
    Collecte statuts DAG, durées, retries et taux d'échec.
    """
    snapshot: dict[str, Any] = {
        "dags": [],
        "summary": {
            "total_dags": 0,
            "failed_runs": 0,
            "success_runs": 0,
            "total_task_failures": 0,
            "avg_task_duration_sec": 0.0,
        },
    }

    durations: list[float] = []
    failed_runs = 0
    success_runs = 0
    task_failures = 0

    for dag_id in WIKIMEDIA_DAG_IDS:
        dag_info: dict[str, Any] = {
            "dag_id": dag_id,
            "is_paused": None,
            "last_run_state": None,
            "last_run_id": None,
            "task_instances": [],
            "failure_rate_pct": 0.0,
        }
        try:
            dag_meta = _api_get(f"/dags/{dag_id}")
            dag_info["is_paused"] = dag_meta.get("is_paused")
        except urllib.error.URLError as exc:
            dag_info["error"] = str(exc)
            snapshot["dags"].append(dag_info)
            continue

        try:
            runs_payload = _api_get(
                f"/dags/{dag_id}/dagRuns?limit=5&order_by=-start_date"
            )
            runs = runs_payload.get("dag_runs", runs_payload)
            if isinstance(runs, list) and runs:
                last = runs[0]
                dag_info["last_run_state"] = last.get("state")
                dag_info["last_run_id"] = last.get("dag_run_id")
                if last.get("state") == "failed":
                    failed_runs += 1
                elif last.get("state") == "success":
                    success_runs += 1

                run_id = last.get("dag_run_id")
                if run_id:
                    try:
                        ti_payload = _api_get(
                            f"/dags/{dag_id}/dagRuns/{run_id}/taskInstances"
                        )
                        tasks = ti_payload.get("task_instances", ti_payload)
                        if isinstance(tasks, list):
                            for ti in tasks:
                                state = ti.get("state")
                                duration = ti.get("duration")
                                if state == "failed":
                                    task_failures += 1
                                if duration is not None:
                                    try:
                                        durations.append(float(duration))
                                    except (TypeError, ValueError):
                                        pass
                                dag_info["task_instances"].append(
                                    {
                                        "task_id": ti.get("task_id"),
                                        "state": state,
                                        "duration_sec": duration,
                                        "try_number": ti.get("try_number"),
                                    }
                                )
                    except urllib.error.URLError as exc:
                        dag_info["task_error"] = str(exc)
        except urllib.error.URLError as exc:
            dag_info["runs_error"] = str(exc)

        total_tasks = len(dag_info["task_instances"]) or 1
        failed_tasks = sum(
            1 for t in dag_info["task_instances"] if t.get("state") == "failed"
        )
        dag_info["failure_rate_pct"] = round(
            (failed_tasks / total_tasks) * 100, 2
        )
        snapshot["dags"].append(dag_info)

    snapshot["summary"] = {
        "total_dags": len(WIKIMEDIA_DAG_IDS),
        "failed_runs": failed_runs,
        "success_runs": success_runs,
        "total_task_failures": task_failures,
        "avg_task_duration_sec": round(
            sum(durations) / len(durations), 2
        )
        if durations
        else 0.0,
        "queue_length": "n/a (LocalExecutor)",
        "worker_load": "n/a (LocalExecutor)",
    }
    return snapshot
