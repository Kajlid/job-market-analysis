#!/usr/bin/env bash
# Starts the Hadoop DataNode

# Set Hadoop environment
export HADOOP_HOME=/opt/hadoop
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

echo "Starting DataNode..."

$HADOOP_HOME/sbin/hadoop-daemon.sh start datanode

# Keep container running
sleep infinity
