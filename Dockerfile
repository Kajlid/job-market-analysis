# FROM python:3.9-slim
# FROM python:3.9-slim-bullseye
FROM eclipse-temurin:8-jre-focal

ENV SPARK_VERSION=3.5.1
ENV HADOOP_VERSION=3.2.4
ENV HADOOP_HOME=/opt/hadoop
ENV SPARK_HOME=/opt/spark
ENV JAVA_HOME=/opt/java/openjdk 
ENV PATH=$SPARK_HOME/bin:$PATH
ENV PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
# ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
# ENV PATH=$SPARK_HOME/bin:$PATH:$JAVA_HOME/bin
# ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
# ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# Combine all installation steps into one layer
# RUN apt-get update && apt-get install -y --no-install-recommends \
#     # openjdk-11-jre-headless \
#     # openjdk-17-jdk \
#     # openjdk-11-jdk \
#     # openjdk-8-jre-headless \
#     python3 python3-pip wget && rm -rf /var/lib/apt/lists/* \
#     wget \
#     && wget -qO - https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz | \
#     tar -xz -C /opt \
#     && ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} "$SPARK_HOME" \
#     && rm -rf /var/lib/apt/lists/*
# RUN apt-get update && apt-get install -y --no-install-recommends \
#     python3 \
#     python3-pip \
#     wget \
#     && wget -qO - https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz | \
#     tar -xz -C /opt \
#     && ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} "$SPARK_HOME" \
#     && rm -rf /var/lib/apt/lists/*

# Install Python and wget
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 \
    python3-pip \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Install Hadoop
RUN wget https://downloads.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz \
    && tar -xzf hadoop-${HADOOP_VERSION}.tar.gz -C /opt \
    && mv /opt/hadoop-${HADOOP_VERSION} /opt/hadoop \
    && rm hadoop-${HADOOP_VERSION}.tar.gz

# Download and install Spark separately
# RUN wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz  \
#     && mkdir -p /opt \
#     && tar -xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -C /opt \
#     && ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} "${SPARK_HOME}" \
#     && rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz


RUN wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz \
    && mkdir -p /opt \
    && tar -xzf spark-${SPARK_VERSION}-bin-hadoop3.tgz -C /opt \
    && ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop3 "${SPARK_HOME}" \
    && rm spark-${SPARK_VERSION}-bin-hadoop3.tgz


WORKDIR /app
COPY hdfs/start-hadoop.sh /usr/local/bin/start-hadoop
COPY hdfs/start-hadoop-namenode.sh /usr/local/bin/start-hadoop-namenode
COPY hdfs/start-hadoop-datanode.sh /usr/local/bin/start-hadoop-datanode
COPY requirements.txt .
# RUN pip install --no-cache-dir -r requirements.txt
RUN chmod +x /usr/local/bin/start-hadoop /usr/local/bin/start-hadoop-namenode /usr/local/bin/start-hadoop-datanode
RUN pip3 install --no-cache-dir -r requirements.txt

COPY python_scripts/ /app/python_scripts/

CMD ["python3", "python_scripts/job_analysis.py"]