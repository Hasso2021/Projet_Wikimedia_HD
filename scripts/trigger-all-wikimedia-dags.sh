#!/usr/bin/env bash
set -euo pipefail

dags=(
  wikimedia_ingestion
  wikimedia_hdfs_healthcheck
  wikimedia_global_activity
  wikimedia_top_pages
  wikimedia_user_activity
  wikimedia_bot_activity
  wikimedia_anomaly_detection
  wikimedia_automated_reporting
  wikimedia_pipeline_monitoring
)

echo "[INFO] Activation (unpause) puis déclenchement des DAGs..."

for dag in "${dags[@]}"; do
  echo "  -> $dag"
  docker exec airflow-api-server airflow dags unpause "$dag" 2>/dev/null || true
  docker exec airflow-api-server airflow dags trigger "$dag"
done

echo ""
echo "[OK]   DAGs déclenchés."
echo "       Airflow : http://localhost:8088"
echo "       HDFS    : docker exec hdfs-namenode hdfs dfs -ls -R /data/wikimedia"
