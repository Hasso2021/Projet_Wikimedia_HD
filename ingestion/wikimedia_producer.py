"""
Producteur Wikimedia EventStream → Kafka.

Topics : wm.recentchange.raw, wm.bot.events, wm.page.edits, wm.errors
Classification : bot/humain, anonyme/connecté, type d'événement (edit, new, delete…)

Lancement :
    pip install -r ingestion/requirements.txt
    python ingestion/wikimedia_producer.py --max-events 100
"""

from __future__ import annotations

import argparse
import json
import re
import signal
import sys
from datetime import datetime, timezone
from typing import Any

import requests
import sseclient
from kafka import KafkaProducer
from kafka.errors import KafkaError

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
DEFAULT_KAFKA_BOOTSTRAP = "localhost:9092"

TOPIC_VALID = "wm.recentchange.raw"
TOPIC_BOT = "wm.bot.events"
TOPIC_PAGE_EDITS = "wm.page.edits"
TOPIC_ERRORS = "wm.errors"
EDIT_EVENT_TYPES = frozenset({"edit", "annotate"})

# User-Agent explicite (exigence Wikimedia)
USER_AGENT = "WikimediaBigData-Master/1.0 (projet Master Big Data; ingestion Kafka)"

# Détection des utilisateurs anonymes (adresse IP comme pseudo)
IP_PATTERN = re.compile(r"^\d{1,3}(\.\d{1,3}){3}$")


# ---------------------------------------------------------------------------
# Arrêt propre (CTRL+C)
# ---------------------------------------------------------------------------

_stop_requested = False


def _handle_sigint(signum, frame) -> None:
    global _stop_requested
    _stop_requested = True
    print("\n[INFO] Arrêt demandé (CTRL+C). Fin du traitement en cours…")


signal.signal(signal.SIGINT, _handle_sigint)


# ---------------------------------------------------------------------------
# Extraction des champs depuis les événements Wikimedia
# ---------------------------------------------------------------------------


def utc_now_iso() -> str:
    """Horodatage UTC ISO-8601 au moment de l'ingestion."""
    return datetime.now(timezone.utc).isoformat()


def extract_wiki(event: dict[str, Any]) -> str | None:
    """
    Récupère le code wiki (enwiki, frwiki, …).
    Wikimedia peut utiliser wiki, server_name ou meta.domain.
    """
    if event.get("wiki"):
        return str(event["wiki"])
    if event.get("server_name"):
        return str(event["server_name"])
    meta = event.get("meta")
    if isinstance(meta, dict) and meta.get("domain"):
        return str(meta["domain"])
    return None


def extract_timestamp(event: dict[str, Any]) -> Any | None:
    """Horodatage événement : champ racine ou meta.dt."""
    if event.get("timestamp") is not None:
        return event["timestamp"]
    meta = event.get("meta")
    if isinstance(meta, dict) and meta.get("dt") is not None:
        return meta["dt"]
    return None


def extract_title(event: dict[str, Any]) -> str | None:
    title = event.get("title")
    if title is None:
        return None
    title = str(title).strip()
    return title if title else None


def extract_user(event: dict[str, Any]) -> str:
    user = event.get("user")
    return str(user) if user is not None else "unknown"


def is_bot_event(event: dict[str, Any]) -> bool:
    return bool(event.get("bot", False))


def is_anonymous_event(event: dict[str, Any]) -> bool:
    """Édition anonyme : flag anonymous, userid=0 ou pseudo = IP."""
    if event.get("anonymous") is True:
        return True
    if event.get("userid") == 0:
        return True
    user = event.get("user")
    if isinstance(user, str) and IP_PATTERN.match(user):
        return True
    return False


def partition_key_from_wiki(wiki: str) -> str:
    """Clé de partition Kafka : on utilise le code wiki (ex. enwiki)."""
    wiki = wiki.strip()
    if wiki.endswith("wiki") and len(wiki) > 4:
        return wiki
    return wiki


# ---------------------------------------------------------------------------
# Validation des événements
# ---------------------------------------------------------------------------


def validate_event(event: dict[str, Any]) -> tuple[bool, str | None]:
    """
    Vérifie title, wiki et timestamp.
    Retourne (True, None) si valide, sinon (False, motif d'erreur).
    """
    title = extract_title(event)
    if not title:
        return False, "missing_or_empty_title"

    wiki = extract_wiki(event)
    if not wiki:
        return False, "missing_wiki"

    timestamp = extract_timestamp(event)
    if timestamp is None:
        return False, "missing_timestamp"

    return True, None


# ---------------------------------------------------------------------------
# Construction des messages Kafka
# ---------------------------------------------------------------------------


def build_valid_record(raw_event: dict[str, Any]) -> dict[str, Any]:
    """Enrichit l'événement Wikimedia (métadonnées + classification)."""
    wiki = extract_wiki(raw_event) or "unknown"
    title = extract_title(raw_event) or "unknown"
    user = extract_user(raw_event)
    page_id = raw_event.get("page_id")
    if page_id is None and isinstance(raw_event.get("meta"), dict):
        page_id = raw_event["meta"].get("id")

    return {
        "ingestion_timestamp": utc_now_iso(),
        "source": "wikimedia",
        "partition_key": partition_key_from_wiki(wiki),
        "is_bot": is_bot_event(raw_event),
        "is_anonymous": is_anonymous_event(raw_event),
        "event_type": str(raw_event.get("type", "unknown")),
        "wiki": wiki,
        "title": title,
        "user": user,
        "page_id": page_id,
        "payload": raw_event,
    }


def publish_valid_event(
    producer: KafkaProducer,
    record: dict[str, Any],
    key: str,
) -> list[str]:
    """
    Route l'événement vers les topics Kafka requis par le sujet.
    Retourne la liste des topics utilisés.
    """
    topics_sent: list[str] = []
    send_to_kafka(producer, TOPIC_VALID, record, key=key)
    topics_sent.append(TOPIC_VALID)

    if record.get("is_bot") is True:
        send_to_kafka(producer, TOPIC_BOT, record, key=key)
        topics_sent.append(TOPIC_BOT)

    event_type = str(record.get("event_type", "")).lower()
    if event_type in EDIT_EVENT_TYPES:
        send_to_kafka(producer, TOPIC_PAGE_EDITS, record, key=key)
        topics_sent.append(TOPIC_PAGE_EDITS)

    return topics_sent


def build_error_record(
    error_reason: str,
    raw_data: str | None = None,
    partial_event: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Enveloppe les messages invalides pour le topic wm.errors."""
    wiki = "unknown"
    if partial_event:
        wiki = extract_wiki(partial_event) or "unknown"

    return {
        "ingestion_timestamp": utc_now_iso(),
        "source": "wikimedia",
        "partition_key": partition_key_from_wiki(wiki) if wiki != "unknown" else "unknown",
        "error_reason": error_reason,
        "raw_data": raw_data,
        "partial_event": partial_event,
    }


# ---------------------------------------------------------------------------
# Connexion Kafka
# ---------------------------------------------------------------------------


def create_kafka_producer(bootstrap_servers: str) -> KafkaProducer:
    """Crée un producteur Kafka (valeurs sérialisées en JSON UTF-8)."""
    print(f"[INFO] Connexion Kafka : {bootstrap_servers} …")
    producer = KafkaProducer(
        bootstrap_servers=[bootstrap_servers],
        value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda key: key.encode("utf-8") if key else None,
        retries=5,
        request_timeout_ms=30000,
    )
    print("[OK]   Producteur Kafka prêt.")
    return producer


def send_to_kafka(
    producer: KafkaProducer,
    topic: str,
    record: dict[str, Any],
    key: str | None,
) -> None:
    """Envoie un message et attend l'accusé de réception (debug)."""
    future = producer.send(topic, value=record, key=key)
    future.get(timeout=10)


# ---------------------------------------------------------------------------
# Boucle principale SSE → validation → Kafka
# ---------------------------------------------------------------------------


def run_producer(
    bootstrap_servers: str,
    stream_url: str,
    max_events: int | None,
) -> None:
    global _stop_requested

    producer = create_kafka_producer(bootstrap_servers)

    stats = {"valid": 0, "invalid": 0, "total": 0}

    print(f"[INFO] Flux Wikimedia : {stream_url}")
    print(
        f"[INFO] Topics : {TOPIC_VALID}, {TOPIC_BOT}, {TOPIC_PAGE_EDITS} | "
        f"Erreurs : {TOPIC_ERRORS}"
    )
    if max_events:
        print(f"[INFO] Arrêt après {max_events} événements (valides + invalides).")
    print("[INFO] CTRL+C pour arrêter proprement.\n")

    headers = {
        "Accept": "text/event-stream",
        "User-Agent": USER_AGENT,
    }

    try:
        response = requests.get(stream_url, stream=True, headers=headers, timeout=60)
        response.raise_for_status()
    except requests.RequestException as exc:
        print(f"[ERROR] Impossible de joindre le flux Wikimedia : {exc}")
        producer.close()
        sys.exit(1)

    client = sseclient.SSEClient(response)

    try:
        for sse_event in client.events():
            if _stop_requested:
                break

            # Lignes vides / keep-alive du flux SSE
            if not sse_event.data:
                continue

            stats["total"] += 1

            # Étape 1 : parser le JSON
            try:
                raw_event = json.loads(sse_event.data)
            except json.JSONDecodeError:
                stats["invalid"] += 1
                error_record = build_error_record(
                    error_reason="invalid_json",
                    raw_data=sse_event.data[:2000],
                )
                send_to_kafka(producer, TOPIC_ERRORS, error_record, key="unknown")
                print(f"[WARN] JSON invalide → {TOPIC_ERRORS}")
                _maybe_stop(stats, max_events)
                continue

            if not isinstance(raw_event, dict):
                stats["invalid"] += 1
                error_record = build_error_record(
                    error_reason="json_not_object",
                    raw_data=sse_event.data[:2000],
                )
                send_to_kafka(producer, TOPIC_ERRORS, error_record, key="unknown")
                print(f"[WARN] JSON non objet → {TOPIC_ERRORS}")
                _maybe_stop(stats, max_events)
                continue

            # Étape 2 : valider les champs obligatoires
            is_valid, error_reason = validate_event(raw_event)

            if is_valid:
                record = build_valid_record(raw_event)
                key = record["partition_key"]
                topics_sent = publish_valid_event(producer, record, key)
                stats["valid"] += 1
                print(
                    f"[OK]   #{stats['total']} {record['event_type']} | "
                    f"{record['wiki']} | {record['title'][:50]} → {', '.join(topics_sent)}"
                )
            else:
                error_record = build_error_record(
                    error_reason=error_reason or "unknown_error",
                    raw_data=sse_event.data[:2000],
                    partial_event=raw_event,
                )
                key = error_record["partition_key"]
                send_to_kafka(producer, TOPIC_ERRORS, error_record, key=key)
                stats["invalid"] += 1
                print(
                    f"[WARN] #{stats['total']} validation échouée "
                    f"({error_reason}) → {TOPIC_ERRORS}"
                )

            _maybe_stop(stats, max_events)

    except KafkaError as exc:
        print(f"[ERROR] Erreur Kafka : {exc}")
    finally:
        print("\n[INFO] Flush et fermeture du producteur…")
        producer.flush()
        producer.close()
        print(
            f"[INFO] Terminé. total={stats['total']} valid={stats['valid']} "
            f"invalid={stats['invalid']}"
        )


def _maybe_stop(stats: dict[str, int], max_events: int | None) -> None:
    """Arrête la boucle quand la limite --max-events est atteinte."""
    global _stop_requested
    if max_events and stats["total"] >= max_events:
        print(f"[INFO] Limite max_events={max_events} atteinte.")
        _stop_requested = True


# ---------------------------------------------------------------------------
# Ligne de commande
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ingère le flux Wikimedia recentchange vers Kafka."
    )
    parser.add_argument(
        "--max-events",
        type=int,
        default=100,
        help="Arrêt après N événements. 0 = illimité.",
    )
    parser.add_argument(
        "--kafka-bootstrap",
        default=DEFAULT_KAFKA_BOOTSTRAP,
        help=f"Bootstrap Kafka (défaut : {DEFAULT_KAFKA_BOOTSTRAP})",
    )
    parser.add_argument(
        "--stream-url",
        default=STREAM_URL,
        help="URL du flux SSE Wikimedia",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    max_events = args.max_events if args.max_events > 0 else None

    print("=" * 60)
    print(" Wikimedia → Kafka")
    print("=" * 60)

    run_producer(
        bootstrap_servers=args.kafka_bootstrap,
        stream_url=args.stream_url,
        max_events=max_events,
    )


if __name__ == "__main__":
    main()
