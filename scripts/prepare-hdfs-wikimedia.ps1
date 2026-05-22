# Crée l'arborescence HDFS pour le pipeline Wikimedia (processed, checkpoints, rapports).
$ErrorActionPreference = "Stop"

Write-Host "[INFO] Création des répertoires HDFS Wikimedia..." -ForegroundColor Cyan

docker exec hdfs-namenode hdfs dfs -mkdir -p /hdfs-data/wikimedia/processed
docker exec hdfs-namenode hdfs dfs -mkdir -p /hdfs-data/wikimedia/checkpoints/stream-consumer
docker exec hdfs-namenode hdfs dfs -mkdir -p /hdfs-data/wikimedia/reports/global
docker exec hdfs-namenode hdfs dfs -mkdir -p /hdfs-data/wikimedia/reports/top_pages
docker exec hdfs-namenode hdfs dfs -mkdir -p /hdfs-data/wikimedia/reports/users
docker exec hdfs-namenode hdfs dfs -mkdir -p /hdfs-data/wikimedia/reports/bots

Write-Host "[OK]   Arborescence créée :" -ForegroundColor Green
docker exec hdfs-namenode hdfs dfs -ls -R /hdfs-data/wikimedia
