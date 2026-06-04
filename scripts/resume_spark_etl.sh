#!/bin/bash
echo "🚀 Resuming Spark ETL for remaining data..."
echo "⏰ Started at: $(date)"

docker exec -it spark_master /spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1,org.postgresql:postgresql:42.2.18 \
  --total-executor-cores 4 \
  --executor-memory 2G \
  --conf spark.sql.legacy.timeParserPolicy=LEGACY \
  --conf spark.sql.shuffle.partitions=16 \
  /opt/spark_etl_resume.py


echo "✅ Completed at: $(date)"
