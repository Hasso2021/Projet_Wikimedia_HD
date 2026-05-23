# Relance les DAGs analytiques (un par un) après échecs WebHDFS / DNS.
$ErrorActionPreference = "Stop"

$dags = @(
    "wikimedia_global_activity",
    "wikimedia_top_pages",
    "wikimedia_user_activity",
    "wikimedia_bot_activity"
)

Write-Host "[INFO] Prérequis : namenode healthy, Spark streaming actif, dossiers HDFS créés." -ForegroundColor Cyan
Write-Host "       .\scripts\prepare-hdfs-wikimedia.ps1" -ForegroundColor DarkGray

foreach ($dag in $dags) {
    Write-Host "  -> $dag" -ForegroundColor Yellow
    docker exec airflow-api-server airflow dags trigger $dag
    Write-Host "     Attente 90s (éviter surcharge DNS Docker)…" -ForegroundColor DarkGray
    Start-Sleep -Seconds 90
}

Write-Host ""
Write-Host "[OK]   Relances envoyées. Vérifiez Airflow : http://localhost:8088" -ForegroundColor Green
Write-Host "       Puis dashboard : Rafraîchir rapports HDFS" -ForegroundColor Green
