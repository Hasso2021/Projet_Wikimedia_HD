# Déclenche tous les DAGs analytiques Wikimedia (après ingestion + Spark).
# IMPORTANT : chaque DAG doit être DÉSACTIVÉ (unpause) — un DAG en pause ne s'exécute pas.
$ErrorActionPreference = "Stop"

$dags = @(
    "wikimedia_hdfs_healthcheck",
    "wikimedia_global_activity",
    "wikimedia_top_pages",
    "wikimedia_user_activity",
    "wikimedia_bot_activity"
)

Write-Host "[INFO] Activation (unpause) puis déclenchement des DAGs..." -ForegroundColor Cyan

foreach ($dag in $dags) {
    Write-Host "  -> $dag" -ForegroundColor Yellow
    # Airflow 3 : un seul DAG par commande unpause
    docker exec airflow-api-server airflow dags unpause $dag
    docker exec airflow-api-server airflow dags trigger $dag
}

Write-Host ""
Write-Host "[OK]   DAGs déclenchés. Attendez 1-2 min puis vérifiez :" -ForegroundColor Green
Write-Host "       Airflow : http://localhost:8088 (runs vertes)" -ForegroundColor Green
Write-Host "       HDFS    : docker exec hdfs-namenode hdfs dfs -ls -R /hdfs-data/wikimedia/reports" -ForegroundColor Green
Write-Host "       Dashboard : http://localhost:8501 (bouton Rafraîchir ou Ctrl+F5)" -ForegroundColor Green
