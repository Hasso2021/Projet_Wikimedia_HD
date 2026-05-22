#!/usr/bin/env bash
# Crée l'arborescence HDFS pour le pipeline Wikimedia.
set -euo pipefail

echo "[INFO] Création des répertoires HDFS Wikimedia..."

docker exec hdfs-namenode hdfs dfs -mkdir -p /hdfs-data/wikimedia/processed
docker exec hdfs-namenode hdfs dfs -mkdir -p /hdfs-data/wikimedia/checkpoints/stream-consumer
docker exec hdfs-namenode hdfs dfs -mkdir -p /hdfs-data/wikimedia/reports/global
docker exec hdfs-namenode hdfs dfs -mkdir -p /hdfs-data/wikimedia/reports/top_pages
docker exec hdfs-namenode hdfs dfs -mkdir -p /hdfs-data/wikimedia/reports/users
docker exec hdfs-namenode hdfs dfs -mkdir -p /hdfs-data/wikimedia/reports/bots

echo "[OK]   Arborescence créée :"
docker exec hdfs-namenode hdfs dfs -ls -R /hdfs-data/wikimedia
