# Déclenche tous les DAGs Wikimedia (ingestion, analytique, anomalies, reporting, monitoring).
$ErrorActionPreference = "Stop"

$dags = @(
    "wikimedia_ingestion",
    "wikimedia_hdfs_healthcheck",
    "wikimedia_global_activity",
    "wikimedia_top_pages",
    "wikimedia_user_activity",
    "wikimedia_bot_activity",
    "wikimedia_anomaly_detection",
    "wikimedia_automated_reporting",
    "wikimedia_pipeline_monitoring"
)

Write-Host "[INFO] Activation (unpause) puis déclenchement des DAGs..." -ForegroundColor Cyan

foreach ($dag in $dags) {
    Write-Host "  -> $dag" -ForegroundColor Yellow
    docker exec airflow-api-server airflow dags unpause $dag
    docker exec airflow-api-server airflow dags trigger $dag
}

Write-Host ""
Write-Host "[OK]   DAGs déclenchés. Vérifiez :" -ForegroundColor Green
Write-Host "       Airflow : http://localhost:8088" -ForegroundColor Green
Write-Host "       HDFS    : docker exec hdfs-namenode hdfs dfs -ls -R /data/wikimedia" -ForegroundColor Green
Write-Host "       Dashboard : http://localhost:8501" -ForegroundColor Green
