# Crée l'arborescence HDFS pour le pipeline Wikimedia (processed, checkpoints, rapports).
$ErrorActionPreference = "Stop"

Write-Host "[INFO] Création des répertoires HDFS Wikimedia..." -ForegroundColor Cyan

docker exec hdfs-namenode hdfs dfs -mkdir -p /data/wikimedia/processed
docker exec hdfs-namenode hdfs dfs -mkdir -p /data/wikimedia/checkpoints/stream-consumer
docker exec hdfs-namenode hdfs dfs -mkdir -p /data/wikimedia/anomalies
docker exec hdfs-namenode hdfs dfs -mkdir -p /data/wikimedia/batch
docker exec hdfs-namenode hdfs dfs -mkdir -p /data/wikimedia/reports/global
docker exec hdfs-namenode hdfs dfs -mkdir -p /data/wikimedia/reports/top_pages
docker exec hdfs-namenode hdfs dfs -mkdir -p /data/wikimedia/reports/users
docker exec hdfs-namenode hdfs dfs -mkdir -p /data/wikimedia/reports/bots
docker exec hdfs-namenode hdfs dfs -mkdir -p /data/wikimedia/reports/quality
docker exec hdfs-namenode hdfs dfs -mkdir -p /data/wikimedia/reports/traffic
docker exec hdfs-namenode hdfs dfs -mkdir -p /data/wikimedia/reports/system
docker exec hdfs-namenode hdfs dfs -mkdir -p /data/wikimedia/reports/monitoring

Write-Host "[OK]   Arborescence créée :" -ForegroundColor Green
docker exec hdfs-namenode hdfs dfs -ls -R /data/wikimedia
