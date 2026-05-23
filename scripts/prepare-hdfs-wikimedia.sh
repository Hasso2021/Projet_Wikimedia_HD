#!/usr/bin/env bash
# Crée l'arborescence HDFS pour le pipeline Wikimedia (/data/wikimedia).
set -euo pipefail

echo "[INFO] Création des répertoires HDFS Wikimedia..."

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

echo "[OK]   Arborescence créée :"
docker exec hdfs-namenode hdfs dfs -ls -R /data/wikimedia
