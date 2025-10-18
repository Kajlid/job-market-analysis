#!/bin/bash

echo "Starting Hadoop..."

# Initialize environment
source /etc/environment

# Format namenode if needed
if [ "$1" = "namenode" ]; then
    if [ ! -d "/opt/hdfs/name/current" ]; then
        echo "Formatting namenode directory"
        hdfs namenode -format -force
    fi
    hdfs --daemon start namenode
elif [ "$1" = "datanode" ]; then
    hdfs --daemon start datanode
fi

# Keep container running
tail -f /dev/null