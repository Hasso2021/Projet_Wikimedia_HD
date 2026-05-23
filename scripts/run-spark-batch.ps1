# Job Spark Batch — résumé agrégé sur HDFS /data/wikimedia/batch/
docker exec -it spark-master spark-submit `
  --master spark://spark-master:7077 `
  --conf spark.hadoop.fs.defaultFS=hdfs://namenode:8020 `
  /opt/bitnami/spark/jobs/wikimedia_batch_processor.py
