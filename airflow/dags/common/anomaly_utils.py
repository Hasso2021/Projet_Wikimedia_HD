"""
Détection d'anomalies sur les événements Wikimedia.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any

from common.wikimedia_hdfs_utils import (
    event_timestamp,
    page_key,
    wiki_to_language,
)

# Seuils du sujet
EDIT_SPIKE_RATIO = 3.0
EDIT_SPIKE_WINDOW_MIN = 5
BOT_EDITS_PER_MIN_THRESHOLD = 30
SPAM_EDITS_IN_WINDOW = 5
SPAM_WINDOW_MIN = 2
SHORT_TITLE_LEN = 5


def _severity(count: int, high: int = 10, medium: int = 5) -> str:
    if count >= high:
        return "high"
    if count >= medium:
        return "medium"
    return "low"


def detect_anomalies(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Détecte les anomalies DATA QUALITY + OPS sur la fenêtre d'événements chargée.
    """
    anomalies: list[dict[str, Any]] = []
    now = datetime.now(timezone.utc)

    page_edits_by_window: dict[tuple[str, str], list[datetime]] = defaultdict(list)
    bot_edits_by_minute: Counter = Counter()
    page_edit_counts: Counter = Counter()
    title_counter: Counter = Counter()

    for event in events:
        title, wiki = page_key(event)
        user = str(event.get("user") or "unknown")
        ts = event_timestamp(event) or now

        # --- Anomalies données ---
        if event.get("wiki") is None or str(event.get("wiki", "")).strip() == "":
            anomalies.append(
                {
                    "type": "data_anomaly",
                    "page": title,
                    "severity": "high",
                    "timestamp": ts.isoformat(),
                    "details": {"reason": "wiki_null", "wiki": event.get("wiki")},
                }
            )

        lang = wiki_to_language(event.get("wiki"))
        if lang == "unknown" and event.get("wiki"):
            anomalies.append(
                {
                    "type": "data_anomaly",
                    "page": title,
                    "severity": "medium",
                    "timestamp": ts.isoformat(),
                    "details": {"reason": "unknown_language", "wiki": event.get("wiki")},
                }
            )

        if event.get("page_id") is None and event.get("payload"):
            payload = event.get("payload") or {}
            if isinstance(payload, dict) and payload.get("page_id") is None:
                anomalies.append(
                    {
                        "type": "data_anomaly",
                        "page": title,
                        "severity": "medium",
                        "timestamp": ts.isoformat(),
                        "details": {"reason": "missing_page_id"},
                    }
                )

        ing_ts = event_timestamp(event)
        if ing_ts and ing_ts > now + timedelta(minutes=5):
            anomalies.append(
                {
                    "type": "data_anomaly",
                    "page": title,
                    "severity": "high",
                    "timestamp": ts.isoformat(),
                    "details": {"reason": "incoherent_timestamp", "ingestion_timestamp": str(event.get("ingestion_timestamp"))},
                }
            )

        # --- Comptages pour heuristiques ---
        page_key_str = f"{title}|{wiki}"
        page_edits_by_window[page_key_str].append(ts)
        page_edit_counts[page_key_str] += 1
        title_counter[str(event.get("title") or "")] += 1

        if event.get("is_bot") is True:
            bot_edits_by_minute[ts.strftime("%Y-%m-%d %H:%M")] += 1

        # Spam : titre très court répété
        if len(str(event.get("title") or "")) <= SHORT_TITLE_LEN:
            anomalies.append(
                {
                    "type": "spam_heuristic",
                    "page": title,
                    "severity": "low",
                    "timestamp": ts.isoformat(),
                    "details": {"reason": "short_title_repeated", "title": title, "user": user},
                }
            )

    # --- Spike d'activité (>300% sur fenêtre 5 min) ---
    for page_key_str, timestamps in page_edits_by_window.items():
        if len(timestamps) < 4:
            continue
        timestamps.sort()
        window = timedelta(minutes=EDIT_SPIKE_WINDOW_MIN)
        for i in range(len(timestamps)):
            window_end = timestamps[i] + window
            in_window = [t for t in timestamps if timestamps[i] <= t <= window_end]
            if len(in_window) < 3:
                continue
            baseline = max(1, len(timestamps) / max(1, (timestamps[-1] - timestamps[0]).total_seconds() / 60))
            if len(in_window) >= baseline * EDIT_SPIKE_RATIO:
                title, wiki = page_key_str.split("|", 1)
                anomalies.append(
                    {
                        "type": "edit_spike",
                        "page": title,
                        "severity": _severity(len(in_window)),
                        "timestamp": in_window[-1].isoformat(),
                        "details": {
                            "edit_count": len(in_window),
                            "window": f"{EDIT_SPIKE_WINDOW_MIN}min",
                            "wiki": wiki,
                            "ratio_vs_baseline": round(len(in_window) / baseline, 2),
                        },
                    }
                )
                break

    # --- Vandalisme : >5 edits en 2 min ---
    for page_key_str, timestamps in page_edits_by_window.items():
        timestamps.sort()
        window = timedelta(minutes=SPAM_WINDOW_MIN)
        for i in range(len(timestamps)):
            window_end = timestamps[i] + window
            in_window = [t for t in timestamps if timestamps[i] <= t <= window_end]
            if len(in_window) >= SPAM_EDITS_IN_WINDOW:
                title, wiki = page_key_str.split("|", 1)
                anomalies.append(
                    {
                        "type": "spam_heuristic",
                        "page": title,
                        "severity": "high",
                        "timestamp": in_window[-1].isoformat(),
                        "details": {
                            "edit_count": len(in_window),
                            "window": f"{SPAM_WINDOW_MIN}min",
                            "wiki": wiki,
                            "reason": "rapid_page_edits",
                        },
                    }
                )
                break

    # --- Bot behavior anomaly ---
    for minute_key, count in bot_edits_by_minute.items():
        if count >= BOT_EDITS_PER_MIN_THRESHOLD:
            anomalies.append(
                {
                    "type": "bot_behavior_anomaly",
                    "page": "multiple",
                    "severity": "high",
                    "timestamp": f"{minute_key}:00+00:00",
                    "details": {
                        "edits_per_minute": count,
                        "threshold": BOT_EDITS_PER_MIN_THRESHOLD,
                        "reason": "bot_high_edit_rate",
                    },
                }
            )

    # Déduplication simple par (type, page, timestamp)
    seen: set[tuple[str, str, str]] = set()
    unique: list[dict[str, Any]] = []
    for item in anomalies:
        key = (item["type"], item["page"], item["timestamp"])
        if key not in seen:
            seen.add(key)
            unique.append(item)

    return unique
