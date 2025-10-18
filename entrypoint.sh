#!/bin/bash
set -e

# Wait for MongoDB
until nc -z mongodb 27017; do
  echo "Waiting for MongoDB..."
  sleep 2
done

# Wait for HDFS namenode
until hdfs dfs -ls / 2>/dev/null; do
  echo "Waiting for HDFS..."
  sleep 2
done

# Activate virtualenv if exists
if [ -d "/app/venv" ]; then
  source /app/venv/bin/activate
else
  python3 -m venv /app/venv
  apt-get update && apt-get install -y python3-venv python3-pip
  source /app/venv/bin/activate
  pip install --no-cache-dir -r requirements.txt
fi

# Run spark-submit
/opt/spark/bin/spark-submit \
  --master local[*] \
  --conf spark.executorEnv.PYTHONPATH=/app/venv/lib/python3.12/site-packages \
  python_scripts/job_analysis.py
