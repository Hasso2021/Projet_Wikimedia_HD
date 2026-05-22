#!/usr/bin/env bash
set -euo pipefail

dags=(
  wikimedia_hdfs_healthcheck
  wikimedia_global_activity
  wikimedia_top_pages
  wikimedia_user_activity
  wikimedia_bot_activity
)

echo "[INFO] Activation et déclenchement des DAGs Wikimedia..."

for dag in "${dags[@]}"; do
  echo "  -> $dag"
  docker exec airflow-api-server airflow dags unpause "$dag" 2>/dev/null || true
  docker exec airflow-api-server airflow dags trigger "$dag"
done

echo "[OK]   DAGs déclenchés — suivi : http://localhost:8088"
