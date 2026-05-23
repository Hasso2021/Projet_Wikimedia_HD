"""
Utilitaires HDFS partagés par les DAGs Wikimedia (WebHDFS + parsing JSON).
"""

from __future__ import annotations

import json
import logging
import os
import time
import urllib.error
import urllib.request
from collections import Counter
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

WEBHDFS_BASE = os.getenv("WEBHDFS_BASE", "http://namenode:9870/webhdfs/v1")
WEBHDFS_RETRIES = int(os.getenv("WEBHDFS_RETRIES", "5"))
WEBHDFS_RETRY_DELAY_SEC = float(os.getenv("WEBHDFS_RETRY_DELAY_SEC", "2"))
# Limite de fichiers lus par run (évite timeouts + surcharge DNS quand Spark écrit beaucoup de parts)
ANALYTICS_MAX_PROCESSED_FILES = int(os.getenv("ANALYTICS_MAX_PROCESSED_FILES", "60"))


def _is_transient_url_error(exc: urllib.error.URLError) -> bool:
    """True seulement pour erreurs réseau/DNS — pas pour HTTP 3xx/4xx/5xx."""
    if isinstance(exc, urllib.error.HTTPError):
        return False
    reason = getattr(exc, "reason", None)
    return isinstance(reason, OSError)


def _webhdfs_urlopen(url: str, timeout: int = 30, method: str | None = None, data: bytes | None = None):
    """Appel WebHDFS avec nouvelles tentatives (DNS Docker parfois instable)."""
    last_error: Exception | None = None
    for attempt in range(1, WEBHDFS_RETRIES + 1):
        try:
            request = urllib.request.Request(url, data=data, method=method) if method else None
            if request is not None:
                return urllib.request.urlopen(request, timeout=timeout)
            return urllib.request.urlopen(url, timeout=timeout)
        except urllib.error.HTTPError:
            raise
        except urllib.error.URLError as exc:
            if not _is_transient_url_error(exc):
                raise
            last_error = exc
            if attempt < WEBHDFS_RETRIES:
                logger.warning(
                    "WebHDFS tentative %d/%d échouée (%s), nouvel essai dans %.1fs…",
                    attempt,
                    WEBHDFS_RETRIES,
                    exc,
                    WEBHDFS_RETRY_DELAY_SEC,
                )
                time.sleep(WEBHDFS_RETRY_DELAY_SEC)
    raise last_error  # type: ignore[misc]

# Chemins HDFS du data lake (hdfs://namenode:8020/data/wikimedia/...)
HDFS_DATA_ROOT = "/data/wikimedia"
HDFS_PROCESSED_PATH = f"{HDFS_DATA_ROOT}/processed"
HDFS_REPORTS_ROOT = f"{HDFS_DATA_ROOT}/reports"
HDFS_ANOMALIES_ROOT = f"{HDFS_DATA_ROOT}/anomalies"
HDFS_BATCH_ROOT = f"{HDFS_DATA_ROOT}/batch"
HDFS_CHECKPOINTS_PATH = f"{HDFS_DATA_ROOT}/checkpoints/stream-consumer"

# Ancien chemin du projet — repli si migration non faite
LEGACY_DATA_ROOT = "/hdfs-data/wikimedia"
LEGACY_PROCESSED_PATH = f"{LEGACY_DATA_ROOT}/processed"

REPORT_SUBDIRS = ("top_pages", "users", "bots", "global", "monitoring")


def utc_now_iso() -> str:
    """Horodatage UTC ISO-8601 pour les fichiers de rapport."""
    return datetime.now(timezone.utc).isoformat()


def today_report_date() -> str:
    """Date du rapport journalier (YYYY-MM-DD)."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def webhdfs_list_dir(hdfs_dir: str) -> list[dict[str, Any]]:
    """Liste le contenu d'un répertoire HDFS (op=LISTSTATUS)."""
    url = f"{WEBHDFS_BASE}{hdfs_dir}?op=LISTSTATUS"
    with _webhdfs_urlopen(url, timeout=30) as response:
        payload = json.loads(response.read().decode("utf-8"))
    return payload["FileStatuses"]["FileStatus"]


def webhdfs_path_exists(hdfs_path: str) -> bool:
    """Vérifie qu'un chemin HDFS existe (fichier ou dossier)."""
    url = f"{WEBHDFS_BASE}{hdfs_path}?op=GETFILESTATUS"
    try:
        with _webhdfs_urlopen(url, timeout=15) as response:
            return response.status == 200
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return False
        raise


def webhdfs_open_text(hdfs_path: str) -> str:
    """Lit un fichier HDFS en texte (op=OPEN)."""
    url = f"{WEBHDFS_BASE}{hdfs_path}?op=OPEN"
    with _webhdfs_urlopen(url, timeout=120) as response:
        return response.read().decode("utf-8")


def webhdfs_mkdirs(hdfs_dir: str) -> None:
    """Crée un répertoire HDFS (op=MKDIRS) s'il n'existe pas."""
    path = hdfs_dir.rstrip("/")
    if not path or webhdfs_path_exists(path):
        return
    url = f"{WEBHDFS_BASE}{path}?op=MKDIRS"
    try:
        _webhdfs_urlopen(url, timeout=30, method="PUT")
    except urllib.error.HTTPError as exc:
        if exc.code in (400, 409):
            return
        raise


def webhdfs_write_json(hdfs_path: str, payload: dict[str, Any]) -> str:
    """Écrit un dictionnaire en JSON sur HDFS (CREATE + redirection 307)."""
    parent = os.path.dirname(hdfs_path.rstrip("/"))
    if parent:
        webhdfs_mkdirs(parent)

    content = json.dumps(payload, ensure_ascii=False, indent=2)
    create_url = f"{WEBHDFS_BASE}{hdfs_path}?op=CREATE&overwrite=true"
    request = urllib.request.Request(create_url, method="PUT")
    try:
        urllib.request.urlopen(request, timeout=30)
    except urllib.error.HTTPError as error:
        if error.code != 307:
            raise
        redirect_url = error.headers.get("Location")
        if not redirect_url:
            raise RuntimeError(f"WebHDFS CREATE sans Location : {hdfs_path}") from error
        _webhdfs_urlopen(
            redirect_url,
            timeout=120,
            method="PUT",
            data=content.encode("utf-8"),
        )
    logger.info("Fichier HDFS écrit : %s", hdfs_path)
    return hdfs_path


def _discover_json_under(root: str) -> list[str]:
    """Parcourt un répertoire HDFS et liste les fichiers .json."""
    json_files: list[str] = []
    if not webhdfs_path_exists(root):
        return json_files

    directories_to_visit = [root.rstrip("/")]
    while directories_to_visit:
        current = directories_to_visit.pop()
        try:
            entries = webhdfs_list_dir(current)
        except urllib.error.HTTPError:
            break

        for entry in entries:
            name = entry["pathSuffix"]
            full_path = f"{current}/{name}"
            if entry["type"] == "DIRECTORY":
                if not name.startswith("_"):
                    directories_to_visit.append(full_path)
            elif entry["type"] == "FILE" and name.endswith(".json"):
                json_files.append(full_path)

    return sorted(json_files)


def discover_processed_json_files() -> list[str]:
    """Liste les JSON processed (chemin courant, repli legacy)."""
    files = _discover_json_under(HDFS_PROCESSED_PATH)
    if files:
        return files
    return _discover_json_under(LEGACY_PROCESSED_PATH)


def discover_anomaly_json_files() -> list[str]:
    """Liste les fichiers d'anomalies sur HDFS."""
    return _discover_json_under(HDFS_ANOMALIES_ROOT)


def parse_events_from_hdfs_file(hdfs_file: str) -> list[dict[str, Any]]:
    """Lit un fichier JSON ligne par ligne (format Spark Streaming)."""
    events: list[dict[str, Any]] = []
    raw_text = webhdfs_open_text(hdfs_file)

    for line_no, line in enumerate(raw_text.splitlines(), start=1):
        line = line.strip()
        if not line:
            continue
        try:
            events.append(json.loads(line))
        except json.JSONDecodeError as exc:
            logger.warning("JSON invalide %s:%d — %s", hdfs_file, line_no, exc)
    return events


def load_all_processed_events() -> tuple[list[dict[str, Any]], list[str]]:
    """Charge tous les événements depuis la zone processed."""
    json_files = discover_processed_json_files()
    if len(json_files) > ANALYTICS_MAX_PROCESSED_FILES:
        skipped = len(json_files) - ANALYTICS_MAX_PROCESSED_FILES
        json_files = json_files[-ANALYTICS_MAX_PROCESSED_FILES:]
        logger.warning(
            "Limite analytics : %d fichier(s) ignoré(s), lecture des %d plus récents.",
            skipped,
            ANALYTICS_MAX_PROCESSED_FILES,
        )
    all_events: list[dict[str, Any]] = []

    for hdfs_file in json_files:
        logger.info("Lecture HDFS : %s", hdfs_file)
        file_events = parse_events_from_hdfs_file(hdfs_file)
        logger.info("  → %d événement(s)", len(file_events))
        all_events.extend(file_events)

    return all_events, json_files


def load_all_anomalies() -> list[dict[str, Any]]:
    """Charge toutes les anomalies enregistrées sur HDFS."""
    anomalies: list[dict[str, Any]] = []
    for hdfs_file in discover_anomaly_json_files():
        if hdfs_file.endswith("anomalies_summary.json"):
            continue
        try:
            payload = json.loads(webhdfs_open_text(hdfs_file))
            if isinstance(payload, dict) and "anomalies" in payload:
                anomalies.extend(payload["anomalies"])
            elif isinstance(payload, list):
                anomalies.extend(payload)
            elif isinstance(payload, dict):
                anomalies.append(payload)
        except (json.JSONDecodeError, urllib.error.URLError) as exc:
            logger.warning("Anomalie non lue %s : %s", hdfs_file, exc)
    return anomalies


def wiki_to_language(wiki: str | None) -> str:
    """Déduit une langue à partir du code wiki (ex. enwiki → en)."""
    if not wiki:
        return "unknown"
    wiki = str(wiki).strip()
    if wiki.endswith("wiki") and len(wiki) > 4:
        return wiki[:-4]
    return wiki


def event_timestamp(event: dict[str, Any]) -> datetime | None:
    """Extrait un horodatage UTC depuis event_time ou ingestion_timestamp."""
    for key in ("event_time", "ingestion_timestamp", "kafka_timestamp"):
        raw = event.get(key)
        if not raw:
            continue
        try:
            normalized = str(raw).strip().replace("Z", "+00:00")
            dt = datetime.fromisoformat(normalized)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except ValueError:
            continue
    return None


def top_n_counter(
    counter: Counter,
    limit: int = 20,
    key_name: str = "key",
    value_name: str = "count",
) -> list[dict[str, Any]]:
    """Transforme un Counter en liste de dicts classés (pour JSON)."""
    return [
        {key_name: key, value_name: count, "rank": rank}
        for rank, (key, count) in enumerate(counter.most_common(limit), start=1)
    ]


def page_key(event: dict[str, Any]) -> tuple[str, str]:
    """Clé (titre, wiki) pour agrégations par page."""
    return (str(event.get("title") or "unknown"), str(event.get("wiki") or "unknown"))


CREATE_TYPES = frozenset({"new", "create"})
DELETE_TYPES = frozenset({"delete", "remove"})
EDIT_TYPES = frozenset({"edit", "annotate"})
