"""
Utilitaires HDFS partagés par les DAGs Wikimedia (WebHDFS + parsing JSON).

Évite de dupliquer le code de lecture/écriture dans chaque DAG Airflow.
"""

from __future__ import annotations

import json
import logging
import urllib.error
import urllib.request
from collections import Counter
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

# Configuration réseau Docker (service namenode)
WEBHDFS_BASE = "http://namenode:9870/webhdfs/v1"
HDFS_PROCESSED_PATH = "/hdfs-data/wikimedia/processed"
HDFS_REPORTS_ROOT = "/hdfs-data/wikimedia/reports"

# Dossiers de rapports attendus par le projet
REPORT_SUBDIRS = ("top_pages", "users", "bots", "global")


def utc_now_iso() -> str:
    """Horodatage UTC ISO-8601 pour les fichiers de rapport."""
    return datetime.now(timezone.utc).isoformat()


def webhdfs_list_dir(hdfs_dir: str) -> list[dict[str, Any]]:
    """Liste le contenu d'un répertoire HDFS (op=LISTSTATUS)."""
    url = f"{WEBHDFS_BASE}{hdfs_dir}?op=LISTSTATUS"
    with urllib.request.urlopen(url, timeout=30) as response:
        payload = json.loads(response.read().decode("utf-8"))
    return payload["FileStatuses"]["FileStatus"]


def webhdfs_path_exists(hdfs_path: str) -> bool:
    """Vérifie qu'un chemin HDFS existe (fichier ou dossier)."""
    url = f"{WEBHDFS_BASE}{hdfs_path}?op=GETFILESTATUS"
    try:
        with urllib.request.urlopen(url, timeout=15) as response:
            return response.status == 200
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return False
        raise


def webhdfs_open_text(hdfs_path: str) -> str:
    """Lit un fichier HDFS en texte (op=OPEN)."""
    url = f"{WEBHDFS_BASE}{hdfs_path}?op=OPEN"
    with urllib.request.urlopen(url, timeout=120) as response:
        return response.read().decode("utf-8")


def webhdfs_write_json(hdfs_path: str, payload: dict[str, Any]) -> str:
    """
    Écrit un dictionnaire en JSON sur HDFS (CREATE + redirection 307).

    Retourne le chemin HDFS écrit.
    """
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
        put_request = urllib.request.Request(
            redirect_url,
            data=content.encode("utf-8"),
            method="PUT",
        )
        urllib.request.urlopen(put_request, timeout=120)
    logger.info("Rapport écrit sur HDFS : %s", hdfs_path)
    return hdfs_path


def discover_processed_json_files() -> list[str]:
    """Parcourt processed/ et retourne tous les fichiers .json (hors _spark_metadata)."""
    json_files: list[str] = []
    directories_to_visit = [HDFS_PROCESSED_PATH.rstrip("/")]

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
    """
    Charge tous les événements depuis la zone processed.

    Retourne (liste d'événements, liste des fichiers lus).
    """
    json_files = discover_processed_json_files()
    all_events: list[dict[str, Any]] = []

    for hdfs_file in json_files:
        logger.info("Lecture HDFS : %s", hdfs_file)
        file_events = parse_events_from_hdfs_file(hdfs_file)
        logger.info("  → %d événement(s)", len(file_events))
        all_events.extend(file_events)

    return all_events, json_files


def wiki_to_language(wiki: str | None) -> str:
    """
    Déduit une langue à partir du code wiki (ex. enwiki → en, frwiki → fr).
    """
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


# Types Wikimedia recentchange courants pour création / suppression
CREATE_TYPES = frozenset({"new", "create"})
DELETE_TYPES = frozenset({"delete", "remove"})
