"""
Job Spark Batch — agrégation des événements processed sur HDFS.

Complète le streaming : lit /data/wikimedia/processed/, écrit un résumé batch.
"""

from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum, when

HDFS_NAMENODE = "hdfs://namenode:8020"
HDFS_PROCESSED = f"{HDFS_NAMENODE}/data/wikimedia/processed"
HDFS_BATCH_OUTPUT = f"{HDFS_NAMENODE}/data/wikimedia/batch/summary"


def main() -> None:
    spark = (
        SparkSession.builder.appName("WikimediaBatchProcessor")
        .config("spark.hadoop.fs.defaultFS", HDFS_NAMENODE)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print(f"[INFO] Lecture batch : {HDFS_PROCESSED}")
    df = spark.read.json(HDFS_PROCESSED)

    if df.rdd.isEmpty():
        print("[WARN] Aucune donnée processed — lancez d'abord le streaming.")
        spark.stop()
        return

    summary = df.agg(
        count("*").alias("total_events"),
        spark_sum(when(col("is_bot") == True, 1).otherwise(0)).alias("bot_events"),
        spark_sum(when(col("is_bot") == False, 1).otherwise(0)).alias("human_events"),
    )

    print("[INFO] Écriture résumé batch…")
    summary.coalesce(1).write.mode("overwrite").json(HDFS_BATCH_OUTPUT)

    wiki_counts = (
        df.groupBy("wiki")
        .agg(count("*").alias("event_count"))
        .orderBy(col("event_count").desc())
    )
    wiki_counts.coalesce(1).write.mode("overwrite").json(
        f"{HDFS_NAMENODE}/data/wikimedia/batch/by_wiki"
    )

    print(f"[OK]   Batch terminé → {HDFS_BATCH_OUTPUT}")
    spark.stop()


if __name__ == "__main__":
    main()
