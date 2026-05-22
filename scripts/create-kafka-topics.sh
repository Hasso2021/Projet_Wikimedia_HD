#!/usr/bin/env bash
# Crée les topics Kafka Wikimedia (Linux / macOS)
# Prérequis : docker compose up -d (Kafka healthy)

set -euo pipefail

BOOTSTRAP="localhost:9092"

create_topic() {
  local name="$1"
  local partitions="$2"
  docker exec kafka kafka-topics \
    --create \
    --if-not-exists \
    --topic "$name" \
    --bootstrap-server "$BOOTSTRAP" \
    --partitions "$partitions" \
    --replication-factor 1
  echo "[OK]   $name ($partitions partitions)"
}

echo ""
echo "=== Creating Kafka topics ==="
echo ""

create_topic "wm.recentchange.raw" 3
create_topic "wm.bot.events" 3
create_topic "wm.page.edits" 3
create_topic "wm.errors" 1

echo ""
echo "--- Topic list ---"
docker exec kafka kafka-topics --list --bootstrap-server "$BOOTSTRAP"

echo ""
echo "=== Done ==="
echo ""
