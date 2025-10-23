#!/bin/bash
# wait-for-hdfs.sh
# Wait until HDFS Namenode is up

HDFS_NAMENODE_HOST=${1:-namenode}
HDFS_NAMENODE_PORT=${2:-8020}

echo "Waiting for HDFS at $HDFS_NAMENODE_HOST:$HDFS_NAMENODE_PORT..."

until curl -s "http://$HDFS_NAMENODE_HOST:$HDFS_NAMENODE_WEBUI_PORT" > /dev/null; do
  echo "HDFS not ready yet, sleeping 5 seconds..."
  sleep 5
done

echo "HDFS is up!"
exec "$@"
