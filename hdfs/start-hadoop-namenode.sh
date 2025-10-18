#!/usr/bin/env bash
# Starts the Hadoop NameNode

# Set Hadoop environment
export HADOOP_HOME=/opt/hadoop
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

echo "Starting NameNode..."

# Format NameNode if needed
if [ ! -f "/opt/hdfs/name/current/VERSION" ]; then
    echo "Formatting NameNode directory..."
    $HADOOP_HOME/bin/hdfs namenode -format -force
fi

echo "Starting HDFS NameNode service..."
$HADOOP_HOME/sbin/hadoop-daemon.sh start namenode

# Wait for NameNode to be up
while ! $HADOOP_HOME/bin/jps | grep -q 'NameNode'; do
    echo "Waiting for NameNode to start..."
    sleep 5
done

echo "NameNode is running"

# Fix permissions for root directory
$HADOOP_HOME/bin/hdfs dfsadmin -safemode leave
$HADOOP_HOME/bin/hdfs dfs -chown $HDFS_USER:$HDFS_USER /
$HADOOP_HOME/bin/hdfs dfs -chmod 777 /

# Keep container running
sleep infinity
