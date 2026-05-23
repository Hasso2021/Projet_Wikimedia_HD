"""
Dashboard Streamlit — Wikimedia Big Data (deux modes).

1. Historical reports — rapports JSON Airflow sur HDFS (batch).
2. Live streaming — événements en direct depuis Kafka wm.recentchange.raw.

Architecture : sidebar + sélecteur de mode HORS fragment ;
contenu rafraîchi DANS @st.fragment (30s historique / 10s live).
"""

from __future__ import annotations

import json
import logging
import os
import socket
import time
import urllib.error
import urllib.request
from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
import plotly.express as px
import streamlit as st

_logger = logging.getLogger(__name__)


def resolve_kafka_bootstrap() -> str:
    """
    Adresse broker Kafka selon l'environnement.

    - Conteneur Docker : kafka:29092 (listener interne PLAINTEXT)
    - Machine hôte (Streamlit local) : localhost:9092 si le hostname kafka ne résout pas
    """
    explicit = os.getenv("KAFKA_BOOTSTRAP")
    if explicit:
        return explicit.strip()

    host = os.getenv("KAFKA_HOST", "kafka")
    port = int(os.getenv("KAFKA_PORT", "29092"))

    try:
        socket.getaddrinfo(host, port)
        return f"{host}:{port}"
    except OSError:
        return "localhost:9092"


def _deserialize_event(raw: bytes | None) -> dict[str, Any] | None:
    if not raw:
        return None
    payload = json.loads(raw.decode("utf-8"))
    return payload if isinstance(payload, dict) else None


def _enrich_kafka_message(message: Any) -> dict[str, Any] | None:
    """Ajoute partition/offset Kafka pour dédoublonnage et debug."""
    payload = message.value if hasattr(message, "value") else message
    if not isinstance(payload, dict):
        return None
    enriched = dict(payload)
    if hasattr(message, "partition"):
        enriched["_kafka_partition"] = message.partition
    if hasattr(message, "offset"):
        enriched["_kafka_offset"] = message.offset
    return enriched


def _poll_kafka_new_only(
    bootstrap_servers: str,
    topic: str,
    max_messages: int,
    timeout_ms: int,
    group_id: str,
) -> list[dict[str, Any]]:
    """
    Mode live normal : groupe consumer dédié à la session, offset latest au premier join.

    Chaque poll lit uniquement les messages arrivés depuis le commit précédent
    (pas de relecture du début du topic).
    """
    from kafka import KafkaConsumer

    events: list[dict[str, Any]] = []
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        consumer_timeout_ms=timeout_ms,
        max_poll_records=max_messages,
        value_deserializer=_deserialize_event,
    )
    try:
        for message in consumer:
            row = _enrich_kafka_message(message)
            if row:
                events.append(row)
            if len(events) >= max_messages:
                break
    finally:
        consumer.close()

    return events


def _poll_kafka_from_beginning(
    bootstrap_servers: str,
    topic: str,
    max_messages: int,
    timeout_ms: int,
) -> list[dict[str, Any]]:
    """
    Mode debug : lecture depuis le début du topic (assign + seek 0), sans groupe consumer.
    """
    from kafka import KafkaConsumer, TopicPartition

    events: list[dict[str, Any]] = []
    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap_servers,
        consumer_timeout_ms=timeout_ms,
        value_deserializer=_deserialize_event,
    )
    try:
        partitions = consumer.partitions_for_topic(topic)
        if not partitions:
            return events

        tps = [TopicPartition(topic, p) for p in sorted(partitions)]
        consumer.assign(tps)
        for tp in tps:
            consumer.seek(tp, 0)

        for message in consumer:
            row = _enrich_kafka_message(message)
            if row:
                events.append(row)
            if len(events) >= max_messages:
                break
    finally:
        consumer.close()

    return events


def poll_kafka_events(
    bootstrap_servers: str,
    topic: str,
    max_messages: int = 100,
    timeout_ms: int = 3000,
    group_id: str = "streamlit-wikimedia-live",
    read_mode: str = "new_only",
) -> tuple[list[dict[str, Any]], str | None]:
    """
    Lit jusqu'à max_messages événements depuis Kafka puis se déconnecte.

    read_mode :
      - new_only : nouveaux messages (groupe consumer, auto_offset_reset=latest)
      - debug_from_beginning : relecture depuis le début (debug uniquement)

    consumer_timeout_ms évite de bloquer Streamlit.
    """
    try:
        from kafka import KafkaConsumer  # noqa: F401 — vérifie l'installation
    except ImportError:
        return [], "Package kafka-python manquant (pip install kafka-python)."

    try:
        if read_mode == "debug_from_beginning":
            events = _poll_kafka_from_beginning(
                bootstrap_servers, topic, max_messages, timeout_ms
            )
        else:
            events = _poll_kafka_new_only(
                bootstrap_servers, topic, max_messages, timeout_ms, group_id
            )
        _logger.info("Kafka poll (%s) : %d message(s).", read_mode, len(events))
        return events, None
    except Exception as exc:
        _logger.warning("Erreur Kafka : %s", exc)
        return [], f"Kafka indisponible ({bootstrap_servers}) : {exc}"

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

WEBHDFS_BASE = os.getenv("WEBHDFS_BASE", "http://namenode:9870/webhdfs/v1")
HDFS_DATA_ROOT = os.getenv("HDFS_DATA_ROOT", "/data/wikimedia")
HDFS_REPORTS_ROOT = os.getenv("HDFS_REPORTS_PATH", f"{HDFS_DATA_ROOT}/reports")
HDFS_ANOMALIES_ROOT = f"{HDFS_DATA_ROOT}/anomalies"
AIRFLOW_API_BASE = os.getenv(
    "AIRFLOW_API_BASE",
    "http://airflow-api-server:8080/api/v2",
).rstrip("/")
AIRFLOW_ADMIN_USER = os.getenv("AIRFLOW_ADMIN_USER", "admin")
AIRFLOW_ADMIN_PASSWORD = os.getenv("AIRFLOW_ADMIN_PASSWORD", "admin")

KAFKA_HOST = os.getenv("KAFKA_HOST", "kafka")
KAFKA_PORT = int(os.getenv("KAFKA_PORT", "29092"))
SPARK_UI_URL = os.getenv("SPARK_UI_URL", "http://spark-master:8080")
AIRFLOW_HEALTH_URL = os.getenv(
    "AIRFLOW_HEALTH_URL",
    "http://airflow-api-server:8080/api/v2/monitor/health",
)

REFRESH_SECONDS = int(os.getenv("DASHBOARD_REFRESH_SECONDS", "30"))
FRESHNESS_THRESHOLD_MINUTES = int(os.getenv("REPORT_FRESHNESS_MINUTES", "10"))
PARIS_TZ = ZoneInfo("Europe/Paris")

# Kafka — Docker : kafka:29092 | hôte Windows : localhost:9092 (auto-détection)
KAFKA_BOOTSTRAP = resolve_kafka_bootstrap()
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "wm.recentchange.raw")
LIVE_REFRESH_SECONDS = int(os.getenv("LIVE_REFRESH_SECONDS", "10"))
LIVE_MAX_EVENTS = int(os.getenv("LIVE_MAX_EVENTS", "1000"))
LIVE_STALE_SECONDS = int(os.getenv("LIVE_STALE_SECONDS", "30"))

LIVE_READ_NEW_ONLY = "Lire nouveaux messages uniquement"
LIVE_READ_DEBUG_BEGIN = "Lire depuis le début pour debug"
LIVE_POLL_MAX_MESSAGES = int(os.getenv("LIVE_POLL_MAX_MESSAGES", "100"))
LIVE_POLL_TIMEOUT_MS = int(os.getenv("LIVE_POLL_TIMEOUT_MS", "3000"))
LIVE_TOP_PAGES_N = int(os.getenv("LIVE_TOP_PAGES_N", "15"))
LIVE_PAGE_TITLE_MAX_LEN = int(os.getenv("LIVE_PAGE_TITLE_MAX_LEN", "50"))

MODE_HISTORICAL = "Historical reports"
MODE_LIVE = "Live streaming"
MODE_OPS = "Monitoring & Ops"

PLOT_LAYOUT = dict(
    template="plotly_dark",
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(color="#e0e0e0"),
)

ARCHITECTURE_TEXT = (
    "**Wikimedia SSE** → **Kafka** → **Spark (Streaming + Batch)** → "
    "**HDFS** `/data/wikimedia/` → **Airflow (9 DAGs)** → **Streamlit**"
)

# Catalogue des rapports par DAG (chemins HDFS relatifs à reports/)
REPORT_CATALOG: dict[str, str] = {
    "health": f"{HDFS_REPORTS_ROOT}/hdfs_health_report.json",
    "global_activity": f"{HDFS_REPORTS_ROOT}/global/global_activity.json",
    "activity_by_minute": f"{HDFS_REPORTS_ROOT}/global/activity_by_minute.json",
    "activity_by_hour": f"{HDFS_REPORTS_ROOT}/global/activity_by_hour.json",
    "language_distribution": f"{HDFS_REPORTS_ROOT}/global/language_distribution.json",
    "activity_by_wiki": f"{HDFS_REPORTS_ROOT}/global/activity_by_wiki.json",
    "top_pages": f"{HDFS_REPORTS_ROOT}/top_pages/top_pages.json",
    "most_created_pages": f"{HDFS_REPORTS_ROOT}/top_pages/most_created_pages.json",
    "most_deleted_pages": f"{HDFS_REPORTS_ROOT}/top_pages/most_deleted_pages.json",
    "user_activity": f"{HDFS_REPORTS_ROOT}/users/user_activity.json",
    "anonymous_vs_logged": f"{HDFS_REPORTS_ROOT}/users/anonymous_vs_logged.json",
    "bot_ratio": f"{HDFS_REPORTS_ROOT}/bots/bot_ratio.json",
    "active_bots": f"{HDFS_REPORTS_ROOT}/bots/active_bots.json",
    "anomalies": f"{HDFS_ANOMALIES_ROOT}/anomalies_summary.json",
    "monitoring": f"{HDFS_REPORTS_ROOT}/monitoring/latest_pipeline_monitoring.json",
}

# Anciens chemins (DAG monolithique supprimé) — repli si sous-dossiers absents
LEGACY_REPORT_PATHS: dict[str, str] = {
    "global_activity": f"{HDFS_REPORTS_ROOT}/global_activity.json",
    "activity_by_wiki": f"{HDFS_REPORTS_ROOT}/activity_by_wiki.json",
    "top_pages": f"{HDFS_REPORTS_ROOT}/top_pages.json",
    "bot_ratio": f"{HDFS_REPORTS_ROOT}/bot_ratio.json",
}


# =============================================================================
# Fuseau Europe/Paris
# =============================================================================


def parse_utc_timestamp(iso_string: str | None) -> datetime | None:
    if not iso_string or not str(iso_string).strip():
        return None
    try:
        normalized = str(iso_string).strip().replace("Z", "+00:00")
        dt = datetime.fromisoformat(normalized)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except (ValueError, TypeError):
        return None


def format_paris(iso_string: str | None) -> str:
    dt_utc = parse_utc_timestamp(iso_string)
    if dt_utc is None:
        return "timestamp invalide"
    return dt_utc.astimezone(PARIS_TZ).strftime("%d/%m/%Y %H:%M:%S Europe/Paris")


def format_timestamp_debug(iso_string: str | None) -> tuple[str, str]:
    """Retourne (UTC, Europe/Paris) pour le panneau debug live."""
    dt_utc = parse_utc_timestamp(iso_string)
    if dt_utc is None:
        return "timestamp invalide", "timestamp invalide"
    utc_label = dt_utc.strftime("%d/%m/%Y %H:%M:%S UTC")
    paris_label = dt_utc.astimezone(PARIS_TZ).strftime("%d/%m/%Y %H:%M:%S Europe/Paris")
    return utc_label, paris_label


def now_paris_formatted() -> str:
    return datetime.now(PARIS_TZ).strftime("%d/%m/%Y %H:%M:%S Europe/Paris")


def report_freshness(iso_string: str | None) -> dict[str, Any]:
    dt_utc = parse_utc_timestamp(iso_string)
    if dt_utc is None:
        return {"status": "unknown", "label": "Fraîcheur inconnue", "detail": "Horodatage absent."}
    minutes = (datetime.now(timezone.utc) - dt_utc).total_seconds() / 60.0
    if minutes < FRESHNESS_THRESHOLD_MINUTES:
        return {
            "status": "fresh",
            "label": "Données récentes",
            "detail": f"Rapport il y a {minutes:.0f} min (< {FRESHNESS_THRESHOLD_MINUTES} min).",
        }
    return {
        "status": "stale",
        "label": "Données anciennes",
        "detail": f"Rapport il y a {minutes:.0f} min — relancez les DAGs Airflow.",
    }


# =============================================================================
# Mode Live — état session + poll Kafka
# =============================================================================


def _live_consumer_group_id() -> str:
    """Groupe consumer unique par session Streamlit (évite les offsets obsolètes)."""
    return f"streamlit-wikimedia-live-{st.session_state.live_session_id}"


def _live_read_mode_key() -> str:
    """Convertit le libellé sidebar en clé interne."""
    label = st.session_state.get("live_kafka_read_mode", LIVE_READ_NEW_ONLY)
    if label == LIVE_READ_DEBUG_BEGIN:
        return "debug_from_beginning"
    return "new_only"


def init_session_state() -> None:
    """Initialise les variables Streamlit (mode + tampon d'événements live)."""
    if "dashboard_mode" not in st.session_state:
        st.session_state.dashboard_mode = MODE_HISTORICAL
    if "live_session_id" not in st.session_state:
        st.session_state.live_session_id = str(int(time.time()))
    if "live_events" not in st.session_state:
        st.session_state.live_events = []
    if "live_last_refresh_ts" not in st.session_state:
        st.session_state.live_last_refresh_ts = time.time()
    if "live_events_per_sec" not in st.session_state:
        st.session_state.live_events_per_sec = 0.0
    if "live_total_received" not in st.session_state:
        st.session_state.live_total_received = 0
    if "live_kafka_error" not in st.session_state:
        st.session_state.live_kafka_error = None
    if "live_kafka_read_mode" not in st.session_state:
        st.session_state.live_kafka_read_mode = LIVE_READ_NEW_ONLY
    if "live_last_poll_paris" not in st.session_state:
        st.session_state.live_last_poll_paris = "—"
    if "live_last_poll_count" not in st.session_state:
        st.session_state.live_last_poll_count = 0
    if "live_last_nonempty_poll_ts" not in st.session_state:
        st.session_state.live_last_nonempty_poll_ts = time.time()


def _event_sort_key(event: dict[str, Any]) -> float:
    """Clé de tri décroissante sur ingestion_timestamp (UTC)."""
    dt = parse_utc_timestamp(event.get("ingestion_timestamp"))
    if dt is None:
        return 0.0
    return dt.timestamp()


def _dedupe_events(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Évite les doublons Kafka (partition + offset) dans le tampon."""
    seen: set[tuple[Any, Any]] = set()
    unique: list[dict[str, Any]] = []
    for ev in events:
        key = (ev.get("_kafka_partition"), ev.get("_kafka_offset"))
        if key != (None, None):
            if key in seen:
                continue
            seen.add(key)
        unique.append(ev)
    return unique


def append_live_events(new_events: list[dict[str, Any]]) -> None:
    """
    Fusionne les nouveaux messages, dédoublonne, trie par ingestion_timestamp décroissant.

    Conserve au plus LIVE_MAX_EVENTS événements les plus récents dans le tampon.
    """
    now = time.time()
    elapsed = max(now - st.session_state.live_last_refresh_ts, 0.001)
    batch_size = len(new_events)

    st.session_state.live_last_poll_paris = datetime.now(PARIS_TZ).strftime(
        "%d/%m/%Y %H:%M:%S Europe/Paris"
    )
    st.session_state.live_last_poll_count = batch_size

    if batch_size > 0:
        st.session_state.live_last_nonempty_poll_ts = now

    merged = _dedupe_events(st.session_state.live_events + new_events)
    merged.sort(key=_event_sort_key, reverse=True)
    st.session_state.live_events = merged[:LIVE_MAX_EVENTS]
    st.session_state.live_total_received += batch_size
    st.session_state.live_events_per_sec = round(batch_size / elapsed, 2)
    st.session_state.live_last_refresh_ts = now


def clear_live_buffer() -> None:
    """Vide le tampon live et recrée une session consumer Kafka (offsets remis à zéro)."""
    st.session_state.live_session_id = str(int(time.time()))
    st.session_state.live_events = []
    st.session_state.live_total_received = 0
    st.session_state.live_events_per_sec = 0.0
    st.session_state.live_kafka_error = None
    st.session_state.live_last_poll_count = 0
    st.session_state.live_last_poll_paris = "—"
    st.session_state.live_last_nonempty_poll_ts = time.time()


# =============================================================================
# Chargement HDFS
# =============================================================================


def _webhdfs_read_json(hdfs_path: str) -> dict[str, Any]:
    url = f"{WEBHDFS_BASE}{hdfs_path}?op=OPEN"
    with urllib.request.urlopen(url, timeout=60) as response:
        return json.loads(response.read().decode("utf-8"))


@st.cache_data(ttl=REFRESH_SECONDS, show_spinner=False)
def load_report(key: str) -> dict[str, Any] | None:
    """
    Charge un rapport par clé (chemin multi-DAG puis ancien chemin plat en secours).
    """
    paths_to_try: list[str] = []
    if key in REPORT_CATALOG:
        paths_to_try.append(REPORT_CATALOG[key])
    if key in LEGACY_REPORT_PATHS:
        legacy = LEGACY_REPORT_PATHS[key]
        if legacy not in paths_to_try:
            paths_to_try.append(legacy)

    for path in paths_to_try:
        try:
            return _webhdfs_read_json(path)
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                continue
            raise
    return None


@st.cache_data(ttl=REFRESH_SECONDS, show_spinner=False)
def load_all_reports() -> dict[str, dict[str, Any] | None]:
    """Charge tous les rapports connus (dict clé → JSON ou None)."""
    return {key: load_report(key) for key in REPORT_CATALOG}


def latest_generated_at(reports: dict[str, dict | None]) -> str | None:
    """Retourne le generated_at le plus récent parmi les rapports chargés."""
    timestamps: list[datetime] = []
    for data in reports.values():
        if data and data.get("generated_at"):
            dt = parse_utc_timestamp(data["generated_at"])
            if dt:
                timestamps.append(dt)
    if not timestamps:
        return None
    return max(timestamps).isoformat()


# =============================================================================
# Santé pipeline (services Docker)
# =============================================================================


def _check_tcp(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=3):
            return True
    except OSError:
        return False


def _check_http(url: str) -> bool:
    try:
        with urllib.request.urlopen(url, timeout=5) as response:
            return 200 <= response.status < 400
    except (urllib.error.URLError, TimeoutError, OSError):
        return False


@st.cache_data(ttl=REFRESH_SECONDS, show_spinner=False)
def check_pipeline_status() -> dict[str, dict[str, Any]]:
    checks = {
        "Kafka": (_check_tcp(KAFKA_HOST, KAFKA_PORT), "Port broker 29092"),
        "Spark": (_check_http(SPARK_UI_URL), "UI Spark Master"),
        "Airflow": (_check_http(AIRFLOW_HEALTH_URL), "API health Airflow 3"),
        "HDFS": (
            _check_http(f"{WEBHDFS_BASE}{HDFS_REPORTS_ROOT}?op=LISTSTATUS"),
            "Dossier rapports HDFS",
        ),
    }
    result = {}
    for name, (ok, hint) in checks.items():
        result[name] = {
            "ok": ok,
            "label": "Opérationnel" if ok else "Indisponible",
            "icon": "🟢" if ok else "🔴",
            "hint": hint,
        }
    return result


@st.cache_data(ttl=15, show_spinner=False)
def fetch_spark_apps() -> list[dict[str, Any]]:
    """Liste les applications Spark actives (UI REST)."""
    try:
        url = f"{SPARK_UI_URL.rstrip('/')}/json/"
        with urllib.request.urlopen(url, timeout=5) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
        apps = []
        for app in payload.get("activeapps", []) or []:
            apps.append(
                {
                    "id": app.get("id"),
                    "name": app.get("name"),
                    "cores": app.get("cores"),
                    "memory": app.get("memoryperexecutor"),
                }
            )
        return apps
    except (urllib.error.URLError, json.JSONDecodeError, TimeoutError):
        return []


def _airflow_root_url() -> str:
    if AIRFLOW_API_BASE.endswith("/api/v2"):
        return AIRFLOW_API_BASE[: -len("/api/v2")]
    return "http://airflow-api-server:8080"


@st.cache_data(ttl=3600, show_spinner=False)
def _get_airflow_jwt_token() -> str:
    """Airflow 3 : JWT via POST /auth/token (identifiants admin du .env)."""
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
        raise RuntimeError("Réponse /auth/token sans access_token")
    return token


def _airflow_api_get(path: str, token: str) -> dict[str, Any]:
    url = f"{AIRFLOW_API_BASE}{path}"
    req = urllib.request.Request(
        url,
        headers={"Accept": "application/json", "Authorization": f"Bearer {token}"},
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read().decode("utf-8"))


@st.cache_data(ttl=15, show_spinner=False)
def fetch_airflow_dag_states() -> list[dict[str, Any]]:
    """États des DAGs Wikimedia via API Airflow 3 (authentification JWT)."""
    try:
        token = _get_airflow_jwt_token()
    except Exception as exc:
        return [
            {
                "dag_id": "—",
                "state": f"auth échouée: {exc}",
                "is_paused": "?",
            }
        ]

    dag_ids = [
        "wikimedia_ingestion",
        "wikimedia_hdfs_healthcheck",
        "wikimedia_global_activity",
        "wikimedia_top_pages",
        "wikimedia_user_activity",
        "wikimedia_bot_activity",
        "wikimedia_anomaly_detection",
        "wikimedia_automated_reporting",
        "wikimedia_pipeline_monitoring",
    ]
    rows: list[dict[str, Any]] = []
    for dag_id in dag_ids:
        row = {"dag_id": dag_id, "state": "unknown", "is_paused": "?"}
        try:
            meta = _airflow_api_get(f"/dags/{dag_id}", token)
            row["is_paused"] = meta.get("is_paused")
            runs_data = _airflow_api_get(
                f"/dags/{dag_id}/dagRuns?limit=1&order_by=-start_date",
                token,
            )
            runs = runs_data.get("dag_runs", runs_data)
            if isinstance(runs, list) and runs:
                row["state"] = runs[0].get("state", "unknown")
                row["last_run"] = runs[0].get("start_date", "—")
        except (urllib.error.URLError, json.JSONDecodeError, TimeoutError) as exc:
            row["state"] = f"erreur: {exc}"
        rows.append(row)
    return rows


@st.cache_data(ttl=10, show_spinner=False)
def fetch_kafka_ops_metrics() -> dict[str, Any]:
    """Lag Kafka et volumes par topic."""
    try:
        from kafka import KafkaConsumer, TopicPartition
    except ImportError:
        return {"status": "kafka-python manquant", "total_lag": 0, "topics": {}}

    topics = ["wm.recentchange.raw", "wm.bot.events", "wm.page.edits", "wm.errors"]
    volumes: dict[str, int] = {}
    total_lag = 0
    try:
        consumer = KafkaConsumer(
            bootstrap_servers=[KAFKA_BOOTSTRAP],
            group_id="streamlit-ops-monitor",
            enable_auto_commit=False,
        )
        for topic in topics:
            parts = consumer.partitions_for_topic(topic) or set()
            lag_topic = 0
            volume = 0
            for p in parts:
                tp = TopicPartition(topic, p)
                end = consumer.end_offsets([tp]).get(tp, 0)
                committed = consumer.committed(tp) or 0
                lag_topic += max(0, end - committed)
                volume += end
            volumes[topic] = volume
            total_lag += lag_topic
        consumer.close()
        return {"status": "ok", "total_lag": total_lag, "topics": volumes}
    except Exception as exc:
        return {"status": str(exc), "total_lag": -1, "topics": {}}


def fetch_invalid_events_rate(kafka_ops: dict[str, Any], total_processed: int) -> float:
    """Taux d'événements invalides (topic wm.errors vs volume total)."""
    errors = (kafka_ops.get("topics") or {}).get("wm.errors", 0)
    denom = max(1, errors + total_processed)
    return round((errors / denom) * 100, 2)


def render_ops_dashboard_content() -> None:
    """Dashboard Monitoring & Ops — santé pipeline."""
    st.markdown("### Monitoring & Ops — pipeline complet")

    pipeline = check_pipeline_status()
    kafka_ops = fetch_kafka_ops_metrics()
    spark_apps = fetch_spark_apps()
    dag_states = fetch_airflow_dag_states()
    reports = load_all_reports()
    ga = reports.get("global_activity") or {}
    total_processed = int(ga.get("total_events") or 0)
    anomalies_report = load_report("anomalies")
    monitoring_report = load_report("monitoring")
    invalid_rate = fetch_invalid_events_rate(kafka_ops, total_processed)

    st.markdown("#### Streaming (temps réel)")
    events = st.session_state.get("live_events", [])
    eps = st.session_state.get("live_events_per_sec", 0)
    c1, c2, c3 = st.columns(3)
    c1.metric("events/sec (live)", f"{eps:.1f}")
    c2.metric("Événements tampon live", f"{len(events):,}")
    c3.metric("Bots live (session)", f"{sum(1 for e in events if e.get('is_bot')):,}")

    if events:
        from collections import Counter as _Counter

        page_c = _Counter(
            (str(e.get("title") or "?"), str(e.get("wiki") or "?")) for e in events
        )
        st.markdown("**Top pages live**")
        st.dataframe(
            pd.DataFrame(
                [
                    {"page": f"{t} ({w})", "éditions": c}
                    for (t, w), c in page_c.most_common(10)
                ]
            ),
            use_container_width=True,
            hide_index=True,
        )

    st.markdown("#### Kafka")
    k1, k2 = st.columns(2)
    k1.metric("Consumer lag (total)", f"{kafka_ops.get('total_lag', 0):,}")
    k2.metric("Statut broker", kafka_ops.get("status", "—"))
    if kafka_ops.get("topics"):
        st.dataframe(
            pd.DataFrame(
                [{"topic": t, "volume_estimé": v} for t, v in kafka_ops["topics"].items()]
            ),
            use_container_width=True,
            hide_index=True,
        )

    st.markdown("#### Spark")
    if spark_apps:
        st.dataframe(pd.DataFrame(spark_apps), use_container_width=True, hide_index=True)
        st.caption("Jobs Spark actifs (Master UI).")
    else:
        st.info("Aucune app Spark active visible — lancez spark-submit streaming ou batch.")

    st.markdown("#### Airflow")
    if dag_states:
        st.dataframe(pd.DataFrame(dag_states), use_container_width=True, hide_index=True)
        failures = sum(1 for d in dag_states if d.get("state") == "failed")
        st.metric("DAG runs en échec (dernier run)", failures)

    st.markdown("#### Data Quality")
    anomaly_count = 0
    if anomalies_report and isinstance(anomalies_report, dict):
        anomaly_count = int(anomalies_report.get("anomaly_count") or 0)
    d1, d2 = st.columns(2)
    d1.metric("Anomalies détectées", f"{anomaly_count:,}")
    d2.metric("Taux événements invalides", f"{invalid_rate}%")
    if anomalies_report:
        sample = anomalies_report.get("anomalies") or []
        if sample:
            st.json(sample[:5])

    if monitoring_report:
        st.markdown("#### Rapport monitoring pipeline")
        st.json(monitoring_report.get("airflow", {}).get("summary", monitoring_report))


def _plot_style(fig) -> None:
    fig.update_layout(**PLOT_LAYOUT)


def _style_horizontal_bar(fig) -> None:
    """
    Barres horizontales : sans titre sur l'axe Y (évite le chevauchement avec les noms de pages).
    """
    _plot_style(fig)
    fig.update_layout(margin=dict(l=8, r=16, t=48, b=32))
    fig.update_yaxes(
        title_text="",
        automargin=True,
        categoryorder="total ascending",
    )


def _truncate_page_title(title: str, max_len: int = LIVE_PAGE_TITLE_MAX_LEN) -> str:
    """Raccourcit un titre de page pour l'affichage graphique (wiki conservé à part)."""
    title = title.strip()
    if len(title) <= max_len:
        return title
    return title[: max_len - 1].rstrip() + "…"


def _build_live_top_pages_df(
    events: list[dict[str, Any]],
    top_n: int = LIVE_TOP_PAGES_N,
) -> pd.DataFrame:
    """
    Top pages live : comptage entier d'événements par (titre, wiki).

    Équivalent à pandas value_counts() sur la clé (title, wiki) :
    chaque événement du tampon compte pour 1 — pas de normalisation ni de %.
    """
    if not events:
        return pd.DataFrame(columns=["page", "éditions"])

    page_keys = [
        (str(ev.get("title") or "?"), str(ev.get("wiki") or "?"))
        for ev in events
    ]
    counts = Counter(page_keys)

    rows: list[dict[str, Any]] = []
    for (title, wiki), count in counts.most_common(top_n):
        rows.append(
            {
                "page": f"{_truncate_page_title(title)} ({wiki})",
                "éditions": int(count),
            }
        )

    df = pd.DataFrame(rows)
    if not df.empty:
        df["éditions"] = df["éditions"].astype("int64")
    return df


def _style_live_top_pages_chart(fig, max_editions: int) -> None:
    """
    Top pages (live) : axe X en entiers uniquement.

    Plotly choisit parfois des graduations 0.5, 1.0, 1.5 quand le max est petit ;
    on force dtick=1 et tickformat entier car il s'agit de dénombrements.
    """
    _style_horizontal_bar(fig)
    x_max = max(int(max_editions), 1)
    fig.update_xaxes(
        title="Nombre d'éditions",
        tickmode="linear",
        tick0=0,
        dtick=1,
        tickformat="d",
        range=[0, x_max + 1],
        rangemode="tozero",
    )
    fig.update_traces(texttemplate="%{x}", textposition="outside", cliponaxis=False)


def _counter_to_df(data: dict, key_col: str, val_col: str) -> pd.DataFrame:
    if not data:
        return pd.DataFrame(columns=[key_col, val_col])
    return pd.DataFrame(
        [{key_col: k, val_col: v} for k, v in data.items()]
    ).sort_values(val_col, ascending=False)


# =============================================================================
# Sections UI (fragment uniquement)
# =============================================================================


def _section_header_block(reports: dict, pipeline: dict) -> None:
    st.markdown("### Vue d'ensemble")
    st.markdown(ARCHITECTURE_TEXT)

    latest = latest_generated_at(reports)
    freshness = report_freshness(latest)

    c1, c2, c3 = st.columns(3)
    with c1:
        if freshness["status"] == "fresh":
            st.success(f"**{freshness['label']}**")
        elif freshness["status"] == "stale":
            st.warning(f"**{freshness['label']}**")
        else:
            st.error(f"**{freshness['label']}**")
        st.caption(freshness["detail"])
    with c2:
        st.metric("Dernier rapport (Paris)", format_paris(latest))
    with c3:
        st.metric("Actualisation dashboard", now_paris_formatted())

    st.markdown("#### État des services")
    cols = st.columns(4)
    for col, (name, info) in zip(cols, pipeline.items()):
        with col:
            color = "#2ecc71" if info["ok"] else "#e74c3c"
            st.markdown(
                f'<div style="padding:0.8rem;border:2px solid {color};'
                f'border-radius:8px;text-align:center;background:#1a1d24;">'
                f'<div>{info["icon"]}</div><b>{name}</b><br><small>{info["label"]}</small></div>',
                unsafe_allow_html=True,
            )


def _section_global(reports: dict) -> None:
    st.markdown("### 1. Activité générale")
    st.caption("Rapports du DAG `wikimedia_global_activity` — tendances temporelles et répartition.")

    ga = reports.get("global_activity") or {}
    st.metric("Total événements", f"{ga.get('total_events', 0):,}")

    col1, col2 = st.columns(2)

    with col1:
        by_min = (reports.get("activity_by_minute") or {}).get("activity_by_minute", {})
        if by_min:
            df = _counter_to_df(by_min, "minute", "événements").head(60)
            fig = px.line(df, x="minute", y="événements", title="Événements par minute")
            _plot_style(fig)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("activity_by_minute.json absent — lancez wikimedia_global_activity.")

    with col2:
        by_hour = (reports.get("activity_by_hour") or {}).get("activity_by_hour", {})
        if by_hour:
            df = _counter_to_df(by_hour, "heure", "événements").head(48)
            fig = px.bar(df, x="heure", y="événements", title="Événements par heure")
            _plot_style(fig)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("activity_by_hour.json absent.")

    col3, col4 = st.columns(2)
    with col3:
        langs = (reports.get("language_distribution") or {}).get("language_distribution", {})
        if langs:
            df = _counter_to_df(langs, "langue", "événements").head(15)
            fig = px.pie(df, values="événements", names="langue", title="Répartition par langue")
            _plot_style(fig)
            st.plotly_chart(fig, use_container_width=True)

    with col4:
        wikis = (reports.get("activity_by_wiki") or {}).get("events_by_wiki", {})
        if wikis:
            df = _counter_to_df(wikis, "wiki", "événements").head(15)
            fig = px.bar(df, x="wiki", y="événements", title="Événements par wiki")
            fig.update_layout(xaxis_tickangle=-45)
            _plot_style(fig)
            st.plotly_chart(fig, use_container_width=True)


def _section_top_pages(reports: dict) -> None:
    st.markdown("### 2. Top pages")
    st.caption("DAG `wikimedia_top_pages` — pages modifiées, créées et supprimées.")

    def show_pages(data: dict | None, list_key: str, title: str) -> None:
        if not data:
            st.warning(f"{title} : rapport non disponible.")
            return
        rows = data.get(list_key) or data.get("top_pages") or []
        if not rows:
            st.info(f"{title} : aucune donnée.")
            return
        df = pd.DataFrame(rows)
        st.markdown(f"**{title}**")
        st.dataframe(df, use_container_width=True, hide_index=True)
        if "edit_count" in df.columns or "count" in df.columns:
            ycol = "edit_count" if "edit_count" in df.columns else "count"
            label = df["title"] + " (" + df["wiki"] + ")"
            fig = px.bar(x=df[ycol], y=label, orientation="h", title=title, labels={"y": ""})
            _style_horizontal_bar(fig)
            st.plotly_chart(fig, use_container_width=True)

    show_pages(reports.get("top_pages"), "top_pages", "Pages les plus modifiées")
    c1, c2 = st.columns(2)
    with c1:
        show_pages(reports.get("most_created_pages"), "most_created_pages", "Pages les plus créées")
    with c2:
        show_pages(reports.get("most_deleted_pages"), "most_deleted_pages", "Pages les plus supprimées")


def _section_users(reports: dict) -> None:
    st.markdown("### 3. Utilisateurs")
    st.caption("DAG `wikimedia_user_activity` — contributeurs et anonymat.")

    ua = reports.get("user_activity")
    avl = reports.get("anonymous_vs_logged")

    col1, col2 = st.columns(2)

    with col1:
        if ua and ua.get("top_contributors"):
            df = pd.DataFrame(ua["top_contributors"])
            st.markdown("**Top contributeurs (humains)**")
            st.dataframe(df, use_container_width=True, hide_index=True)
            fig = px.bar(
                df.head(15),
                x="edit_count",
                y="user",
                orientation="h",
                title="Top contributeurs",
                labels={"user": ""},
            )
            _style_horizontal_bar(fig)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("user_activity.json absent.")

        if ua and ua.get("bots_vs_humans"):
            bh = ua["bots_vs_humans"]
            st.metric("Bots", f"{bh.get('bots', 0):,}")
            st.metric("Humains", f"{bh.get('humans', 0):,}")
            st.metric("% bots", f"{bh.get('bot_percentage', 0)} %")

    with col2:
        if avl:
            df = pd.DataFrame(
                {
                    "Type": ["Anonymes", "Connectés"],
                    "Nombre": [avl.get("anonymous", 0), avl.get("logged_in", 0)],
                }
            )
            fig = px.pie(df, values="Nombre", names="Type", title="Anonymes vs connectés")
            _plot_style(fig)
            st.plotly_chart(fig, use_container_width=True)
            st.caption(
                f"Anonymes : {avl.get('anonymous_percentage', 0)} % — "
                f"Connectés : {avl.get('logged_in_percentage', 0)} %"
            )
        else:
            st.warning("anonymous_vs_logged.json absent.")


def _section_bots(reports: dict) -> None:
    st.markdown("### 4. Activité bots")
    st.caption("DAG `wikimedia_bot_activity` — volume et bots les plus actifs.")

    br = reports.get("bot_ratio")
    ab = reports.get("active_bots")

    col1, col2 = st.columns(2)

    with col1:
        if br:
            st.metric("Volume bots", f"{br.get('bots', br.get('bot_volume', 0)):,}")
            st.metric("Volume humains", f"{br.get('humans', br.get('human_volume', 0)):,}")
            st.metric("Ratio bots", f"{br.get('bot_percentage', 0)} %")
            df = pd.DataFrame(
                {
                    "Type": ["Bots", "Humains"],
                    "Nombre": [
                        br.get("bots", br.get("bot_volume", 0)),
                        br.get("humans", br.get("human_volume", 0)),
                    ],
                }
            )
            fig = px.pie(df, values="Nombre", names="Type", title="Ratio bot / humain")
            _plot_style(fig)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("bot_ratio.json absent.")

    with col2:
        if ab and ab.get("active_bots"):
            df = pd.DataFrame(ab["active_bots"])
            st.markdown("**Bots les plus actifs**")
            st.dataframe(df, use_container_width=True, hide_index=True)
            fig = px.bar(
                df.head(15),
                x="edit_count",
                y="bot_user",
                orientation="h",
                title="Bots actifs",
                labels={"bot_user": ""},
            )
            _style_horizontal_bar(fig)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("active_bots.json absent.")


def render_dashboard_content() -> None:
    """Contenu rafraîchi par fragment — PAS de st.sidebar ici."""
    try:
        with st.spinner("Chargement des rapports HDFS…"):
            reports = load_all_reports()
            pipeline = check_pipeline_status()
    except urllib.error.URLError as exc:
        st.error(f"HDFS / WebHDFS injoignable : {exc.reason}")
        return
    except Exception as exc:
        st.error(f"Erreur : {exc}")
        return

    loaded = sum(1 for v in reports.values() if v is not None)
    if loaded == 0:
        st.error("Aucun rapport JSON trouvé sur HDFS.")
        st.markdown(
            """
**Checklist (dans l'ordre) :**

1. **Ingestion** : `python ingestion/wikimedia_producer.py --max-events 100`
2. **Spark Streaming** : consumer Kafka → HDFS `processed/`
3. **Activer les DAGs** dans http://localhost:8088 (interrupteur **ON** — un DAG *en pause* ne tourne pas)
4. **Déclencher** : `.\scripts\trigger-all-wikimedia-dags.ps1`
5. Attendre les runs **vertes** (1–2 min), puis **Rafraîchir** ce dashboard

Vérification HDFS :
```powershell
docker exec hdfs-namenode hdfs dfs -ls -R /hdfs-data/wikimedia/reports
```
Vous devez voir les dossiers `global/`, `top_pages/`, `users/`, `bots/`.
            """
        )
        return

    if loaded < 5:
        st.warning(
            f"Seulement **{loaded}/{len(REPORT_CATALOG)}** rapports chargés. "
            "Relancez les DAGs manquants ou attendez la fin des runs Airflow."
        )

    st.info(f"**{loaded}/{len(REPORT_CATALOG)}** rapports chargés depuis HDFS.")

    _section_header_block(reports, pipeline)
    st.divider()
    _section_global(reports)
    st.divider()
    _section_top_pages(reports)
    st.divider()
    _section_users(reports)
    st.divider()
    _section_bots(reports)

    health = reports.get("health")
    if health:
        with st.expander("Rapport santé HDFS (wikimedia_hdfs_healthcheck)"):
            st.json(health)

    st.caption(f"Fuseau : Europe/Paris — refresh auto {REFRESH_SECONDS}s")


# =============================================================================
# Mode Live — sections UI (fragment uniquement)
# =============================================================================


def _live_compute_metrics(events: list[dict[str, Any]]) -> dict[str, Any]:
    """Agrégations simples sur le tampon live_events."""
    bots = sum(1 for e in events if e.get("is_bot") is True)
    humans = len(events) - bots
    by_wiki = Counter(str(e.get("wiki") or "unknown") for e in events)
    by_page = Counter(
        (str(e.get("title") or "?"), str(e.get("wiki") or "?")) for e in events
    )
    return {
        "bots": bots,
        "humans": humans,
        "by_wiki": by_wiki,
        "by_page": by_page,
    }


def _latest_event_timestamps(events: list[dict[str, Any]]) -> tuple[str, str, str]:
    """Dernier ingestion_timestamp du tampon : brut, UTC, Paris."""
    if not events:
        return "—", "—", "—"
    latest = max(events, key=_event_sort_key)
    raw = str(latest.get("ingestion_timestamp") or "—")
    utc_l, paris_l = format_timestamp_debug(latest.get("ingestion_timestamp"))
    return raw, utc_l, paris_l


def render_live_dashboard_content() -> None:
    """
    Mode Live — poll Kafka (non bloquant) + visualisation.

    Ne pas utiliser st.sidebar ici.
    """
    st.markdown("### Mode Live — flux Kafka en direct")
    read_mode = _live_read_mode_key()
    group_id = _live_consumer_group_id()

    st.caption(
        f"Topic **{KAFKA_TOPIC}** — broker **{KAFKA_BOOTSTRAP}** — "
        f"mode **{st.session_state.get('live_kafka_read_mode', LIVE_READ_NEW_ONLY)}**"
    )

    new_events, error = poll_kafka_events(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        topic=KAFKA_TOPIC,
        max_messages=LIVE_POLL_MAX_MESSAGES,
        timeout_ms=LIVE_POLL_TIMEOUT_MS,
        group_id=group_id,
        read_mode=read_mode,
    )
    st.session_state.live_kafka_error = error

    if error:
        st.error(error)
        st.info(
            "Vérifiez Kafka (`docker compose ps kafka`) et le broker :\n"
            f"- Dashboard **dans Docker** → `kafka:29092` (listener interne)\n"
            f"- Dashboard **en local** → `localhost:9092`\n"
            f"- Producteur sur PC → `localhost:9092`\n\n"
            "Lancez le producteur pendant le mode Live :\n"
            "`python ingestion/wikimedia_producer.py --max-events 0`"
        )
        return

    if new_events:
        append_live_events(new_events)
    else:
        # Poll exécuté mais 0 message — horodatage du poll quand même
        st.session_state.live_last_poll_paris = datetime.now(PARIS_TZ).strftime(
            "%d/%m/%Y %H:%M:%S Europe/Paris"
        )
        st.session_state.live_last_poll_count = 0

    events = st.session_state.live_events
    metrics = _live_compute_metrics(events)

    # --- Debug Kafka (visibilité producteur / consumer) ---
    st.markdown("#### Debug Kafka")
    latest_raw, latest_utc, latest_paris = _latest_event_timestamps(events)
    seconds_since_new = time.time() - st.session_state.live_last_nonempty_poll_ts

    d1, d2, d3 = st.columns(3)
    d1.metric("Messages dernier poll", st.session_state.live_last_poll_count)
    d2.metric("Total session", f"{st.session_state.live_total_received:,}")
    d3.metric("Débit dernier poll", f"{st.session_state.live_events_per_sec} evt/s")

    st.markdown(
        f"""
| Indicateur | Valeur |
|------------|--------|
| Dernier poll (Paris) | **{st.session_state.live_last_poll_paris}** |
| Bootstrap | `{KAFKA_BOOTSTRAP}` |
| Topic | `{KAFKA_TOPIC}` |
| Consumer group | `{group_id}` |
| Dernier `ingestion_timestamp` (brut) | `{latest_raw}` |
| Dernier événement (UTC) | {latest_utc} |
| Dernier événement (Paris) | **{latest_paris}** |
        """
    )

    if seconds_since_new > LIVE_STALE_SECONDS:
        st.warning(
            f"⚠ Aucun nouveau message Kafka reçu depuis {LIVE_STALE_SECONDS} secondes. "
            "Vérifiez que le producteur est actif "
            "(`python ingestion/wikimedia_producer.py --max-events 0`). "
            "En mode *nouveaux messages uniquement*, les anciens événements du tampon "
            "ne sont pas rafraîchis tant qu'aucun nouveau message n'arrive."
        )

    if read_mode == "debug_from_beginning":
        st.info(
            "Mode **debug** : relecture depuis le début du topic à chaque poll "
            "(peut recharger des messages déjà vus — dédoublonnage par offset Kafka)."
        )

    # --- 1. Métriques live ---
    st.markdown("#### Métriques live")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Événements en mémoire", f"{len(events):,}")
    c2.metric("Débit (dernier poll)", f"{st.session_state.live_events_per_sec} evt/s")
    c3.metric("Bots", f"{metrics['bots']:,}")
    c4.metric("Humains", f"{metrics['humans']:,}")
    st.caption(
        f"Tampon max {LIVE_MAX_EVENTS} — tri par `ingestion_timestamp` décroissant — "
        f"actualisation {LIVE_REFRESH_SECONDS}s"
    )

    if not events:
        st.warning(
            "Tampon vide. Mode *nouveaux messages uniquement* : seuls les événements "
            "**après** l'ouverture du mode Live sont affichés. Lancez le producteur, "
            "ou passez en mode debug / **Vider tampon live**."
        )
        return

    # --- 2. Derniers événements reçus par le dashboard ---
    st.markdown("#### Derniers événements reçus par le dashboard")
    st.caption(
        "Table triée par date d'ingestion (**plus récent en haut**). "
        "Horodatages convertis depuis l'UTC vers **Europe/Paris** (`zoneinfo`)."
    )
    rows = []
    for ev in events[:30]:
        utc_l, paris_l = format_timestamp_debug(ev.get("ingestion_timestamp"))
        rows.append(
            {
                "title": ev.get("title"),
                "wiki": ev.get("wiki"),
                "user": ev.get("user"),
                "event_type": ev.get("event_type"),
                "is_bot": ev.get("is_bot"),
                "ingestion (UTC)": utc_l,
                "ingestion (Paris)": paris_l,
            }
        )
    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

    # --- 3. Graphiques live ---
    st.markdown("#### Graphiques live")
    col_a, col_b = st.columns(2)

    with col_a:
        wiki_df = pd.DataFrame(
            [{"wiki": w, "événements": int(c)} for w, c in metrics["by_wiki"].most_common(15)]
        )
        if not wiki_df.empty:
            fig = px.bar(wiki_df, x="wiki", y="événements", title="Activité par wiki (live)")
            fig.update_layout(xaxis_tickangle=-45)
            _plot_style(fig)
            wiki_max = int(wiki_df["événements"].max())
            fig.update_yaxes(tickmode="linear", tick0=0, dtick=1, tickformat="d", range=[0, wiki_max + 1])
            st.plotly_chart(fig, use_container_width=True)

        page_df = _build_live_top_pages_df(events, top_n=LIVE_TOP_PAGES_N)
        if not page_df.empty:
            max_editions = int(page_df["éditions"].max())
            fig2 = px.bar(
                page_df,
                x="éditions",
                y="page",
                orientation="h",
                title=f"Top pages (live) — top {LIVE_TOP_PAGES_N}",
                labels={"page": "", "éditions": "Nombre d'éditions"},
            )
            _style_live_top_pages_chart(fig2, max_editions)
            st.plotly_chart(fig2, use_container_width=True)
            st.caption(
                "Chaque barre = **nombre entier d'événements** (éditions) reçus pour cette page "
                f"dans le tampon live (max {LIVE_MAX_EVENTS} événements). "
                "Comptage brut (`Counter` / `value_counts`) : **pas de normalisation**, "
                "**pas de pourcentage**. Les pages sont triées par volume décroissant."
            )

    with col_b:
        pie_df = pd.DataFrame(
            {
                "Type": ["Bots", "Humains"],
                "Nombre": [metrics["bots"], metrics["humans"]],
            }
        )
        fig3 = px.pie(pie_df, values="Nombre", names="Type", title="Bots vs humains (live)")
        _plot_style(fig3)
        st.plotly_chart(fig3, use_container_width=True)

    st.caption(f"Dernière actualisation live : {now_paris_formatted()}")


@st.fragment(run_every=timedelta(seconds=REFRESH_SECONDS))
def auto_refresh_dashboard() -> None:
    render_dashboard_content()


@st.fragment(run_every=timedelta(seconds=LIVE_REFRESH_SECONDS))
def auto_refresh_live() -> None:
    """Fragment mode Live — refresh plus fréquent (10 s par défaut)."""
    render_live_dashboard_content()


def configure_page() -> None:
    st.set_page_config(
        page_title="Wikimedia Big Data — Dashboard",
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="expanded",
    )


def render_page_header() -> None:
    st.title("📊 Wikimedia Big Data — Dashboard")
    mode = st.session_state.get("dashboard_mode", MODE_HISTORICAL)
    if mode == MODE_LIVE:
        st.markdown(
            "**Mode Live** — événements consommés directement depuis **Kafka** "
            "(temps réel). Fuseau : **Europe/Paris**."
        )
    elif mode == MODE_OPS:
        st.markdown(
            "**Mode Monitoring & Ops** — Kafka lag, Spark jobs, états Airflow, "
            "anomalies et qualité des données."
        )
    else:
        st.markdown(
            "**Mode Historical** — rapports **batch** générés par Airflow sur **HDFS**. "
            "Fuseau : **Europe/Paris**."
        )


def render_sidebar() -> None:
    """
    Barre latérale HORS fragment : sélecteur de mode + actions globales.
    """
    with st.sidebar:
        st.header("Paramètres")

        st.markdown("**Mode**")
        st.caption(
            "**Historical** = rapports JSON Airflow (stockage HDFS, batch).  \n"
            "**Live** = flux Kafka direct (temps réel, poll non bloquant)."
        )
        st.radio(
            "Affichage",
            [MODE_HISTORICAL, MODE_LIVE, MODE_OPS],
            key="dashboard_mode",
        )
        mode = st.session_state.dashboard_mode

        st.caption("Fuseau : Europe/Paris")

        if mode == MODE_HISTORICAL:
            st.write(f"Auto-refresh : **{REFRESH_SECONDS} s**")
            if st.button("🔄 Rafraîchir rapports HDFS", key="refresh_hdfs"):
                st.cache_data.clear()
                st.rerun()
            st.divider()
            st.markdown("**DAGs Airflow**")
            for dag in [
                "wikimedia_ingestion",
                "wikimedia_hdfs_healthcheck",
                "wikimedia_global_activity",
                "wikimedia_top_pages",
                "wikimedia_user_activity",
                "wikimedia_bot_activity",
                "wikimedia_anomaly_detection",
                "wikimedia_automated_reporting",
                "wikimedia_pipeline_monitoring",
            ]:
                st.text(f"• {dag}")
        else:
            st.write(f"Auto-refresh : **{LIVE_REFRESH_SECONDS} s**")
            st.markdown("**Lecture Kafka**")
            st.radio(
                "Mode lecture",
                [LIVE_READ_NEW_ONLY, LIVE_READ_DEBUG_BEGIN],
                key="live_kafka_read_mode",
                help=(
                    "Nouveaux messages : groupe consumer session, offset latest — "
                    "recommandé pour la démo live. "
                    "Debug début : relit depuis le début du topic."
                ),
            )
            st.caption(
                "**Nouveaux messages** : n'affiche pas l'historique du topic au chargement.  \n"
                "**Debug début** : utile pour vérifier que Kafka contient des données."
            )
            st.text(f"Topic : {KAFKA_TOPIC}")
            st.text(f"Bootstrap : {KAFKA_BOOTSTRAP}")
            st.text(f"Group : streamlit-wikimedia-live-{st.session_state.live_session_id}")
            if st.button("🗑️ Vider tampon live", key="clear_live"):
                clear_live_buffer()
                st.rerun()

        st.divider()
        st.markdown("**Pipeline**\n\n" + ARCHITECTURE_TEXT.replace("**", ""))


def main() -> None:
    configure_page()
    init_session_state()
    render_page_header()
    render_sidebar()

    mode = st.session_state.dashboard_mode

    if mode == MODE_LIVE:
        if hasattr(st, "fragment"):
            auto_refresh_live()
        else:
            render_live_dashboard_content()
    elif mode == MODE_OPS:
        render_ops_dashboard_content()
        st.caption("Rafraîchissez la page (F5) pour mettre à jour les métriques ops.")
    else:
        if hasattr(st, "fragment"):
            auto_refresh_dashboard()
        else:
            render_dashboard_content()


main()
