#!/usr/bin/env bash

# Starts the Hadoop data node.

source /etc/bash.bashrc

echo "start-hadoop-datanode.sh"

# Start HDFS data node
hdfs --daemon start datanode
# Start daemon if specified
if [[ "${1}" != 'daemon' ]]; then
  sleep infinity
fi