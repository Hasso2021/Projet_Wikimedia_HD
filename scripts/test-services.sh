#!/usr/bin/env bash
# Tests rapides des services Docker (Linux / macOS)
# Usage : ./scripts/test-services.sh

set -euo pipefail

echo ""
echo "=== Big Data Stack — Smoke Tests ==="
echo ""

check_http() {
  local name="$1" url="$2"
  if curl -sf --max-time 15 "$url" -o /dev/null; then
    echo "[OK]   $name — $url"
  else
    echo "[FAIL] $name — $url"
  fi
}

echo "--- Web UIs ---"
check_http "Spark Master"   "http://localhost:8080"
check_http "Spark Worker"   "http://localhost:8081"
check_http "HDFS Namenode"  "http://localhost:9870"
check_http "HDFS Datanode"  "http://localhost:9864"
check_http "Airflow API"    "http://localhost:8088/api/v2/monitor/health"

echo ""
echo "--- Container health ---"
docker compose ps

echo ""
echo "--- Kafka ---"
if docker exec kafka kafka-topics --list --bootstrap-server localhost:9092 >/dev/null 2>&1; then
  echo "[OK]   Kafka broker responded"
else
  echo "[FAIL] Kafka broker not responding"
fi

echo ""
echo "--- HDFS ---"
if docker exec hdfs-namenode hdfs dfs -ls / >/dev/null 2>&1; then
  echo "[OK]   HDFS namenode CLI works"
else
  echo "[FAIL] HDFS namenode CLI failed"
fi

echo ""
echo "--- Airflow DAGs ---"
if docker exec airflow-api-server airflow dags list 2>/dev/null | grep -q hello_world; then
  echo "[OK]   hello_world DAG is visible"
else
  echo "[WARN] Could not find hello_world DAG yet"
fi

echo ""
echo "=== Done. Open http://localhost:8088 for Airflow UI ==="
echo ""
