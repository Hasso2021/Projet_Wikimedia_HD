#!/usr/bin/env bash
set -euo pipefail
docker exec -it spark-master spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.hadoop.fs.defaultFS=hdfs://namenode:8020 \
  /opt/bitnami/spark/jobs/wikimedia_batch_processor.py
