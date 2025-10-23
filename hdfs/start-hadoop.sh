#!/bin/bash

echo "Starting Hadoop..."

# Ensure Hadoop binaries are in PATH
export HADOOP_HOME=/opt/hadoop
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

# Format namenode if needed
if [ "$1" = "namenode" ]; then
    if [ ! -d "/opt/hdfs/name/current" ]; then
        echo "Formatting namenode directory"
        $HADOOP_HOME/bin/hdfs namenode -format -force
    fi
    echo "Starting NameNode daemon"
    $HADOOP_HOME/sbin/hadoop-daemon.sh start namenode

elif [ "$1" = "datanode" ]; then
    echo "Starting DataNode daemon"
    $HADOOP_HOME/sbin/hadoop-daemon.sh start datanode
fi

# Keep container running
echo "Hadoop service is running. Keeping container alive..."
tail -f /dev/null
