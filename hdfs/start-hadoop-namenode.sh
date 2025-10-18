#!/usr/bin/env bash

# Starts the Hadoop name node. Expects the daemon flag as a parameter

source /etc/bash.bashrc

echo "start-hadoop-namenode.sh"

# Format namenode
if [ ! -f /opt/hdfs/name/current/VERSION ]; then
  hdfs namenode -format -force
fi

echo "Namenode formatted, starting HDFS service"

# Start HDFS service
hdfs --daemon start namenode

# Wait for the HDFS NameNode to start
while ! jps | grep -q 'NameNode'; do
  echo "Waiting for NameNode to start..."
  sleep 5
done

echo "NameNode is running"

# Fix permissions for root directory
hdfs dfsadmin -safemode leave
hdfs dfs -chown $HDFS_USER:$HDFS_USER /
# Chmod / so anyone can write to it
hdfs dfs -chmod 777 /

# Start daemon if specified
if [[ "${1}" != 'daemon' ]]; then
  sleep infinity
fi