# FROM ubuntu:latest AS hadoop-base
# FROM ubuntu:20.04 AS hadoop-base
# FROM eclipse-temurin:8-jre-focal
# FROM eclipse-temurin:8-jre-jammy
# FROM ubuntu:22.04
FROM ubuntu:latest

# Prevent interactive prompts during build
# ENV DEBIAN_FRONTEND=noninteractive

# Set versions and paths first
ENV HADOOP_VERSION=3.2.4
# ENV HADOOP_HOME=/opt/hadoop-${HADOOP_VERSION}
ENV HADOOP_HOME=/opt/hadoop
ENV PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
# ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
# ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
# ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# Install only essential packages first

# RUN apt-get update --allow-insecure-repositories && apt-get install -y --no-install-recommends \
#     openjdk-8-jdk-headless \
#     wget \
#     net-tools \
#     && wget -q https://downloads.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz \
#     && tar -xzf hadoop-${HADOOP_VERSION}.tar.gz -C /opt \
#     && rm hadoop-${HADOOP_VERSION}.tar.gz \
#     && rm -rf /var/lib/apt/lists/*

RUN apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get -o Acquire::AllowInsecureRepositories=true \
               -o Acquire::AllowDowngradeToInsecureRepositories=true update \
    && apt-get install -y --no-install-recommends \
       openjdk-8-jdk-headless wget net-tools \
    && rm -rf /var/lib/apt/lists/*

# Add minimal log4j.properties to silence warnings
RUN mkdir -p $HADOOP_HOME/etc/hadoop \
    && echo 'log4j.rootCategory=ERROR, console' > $HADOOP_HOME/etc/hadoop/log4j.properties \
    && echo 'log4j.appender.console=org.apache.log4j.ConsoleAppender' >> $HADOOP_HOME/etc/hadoop/log4j.properties \
    && echo 'log4j.appender.console.target=System.err' >> $HADOOP_HOME/etc/hadoop/log4j.properties \
    && echo 'log4j.appender.console.layout=org.apache.log4j.PatternLayout' >> $HADOOP_HOME/etc/hadoop/log4j.properties \
    && echo 'log4j.appender.console.layout.ConversionPattern=%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n' >> $HADOOP_HOME/etc/hadoop/log4j.properties


# Copy and setup startup scripts
# COPY start-hadoop*.sh /opt/util/bin/
# COPY start-hadoop*.sh /usr/local/bin/
COPY start-hadoop.sh /usr/local/bin/start-hadoop
# RUN chmod +x /opt/util/bin/start-hadoop* \
#     && sed -i 's/\r$//' /opt/util/bin/start-hadoop*
# RUN chmod +x /usr/local/bin/start-hadoop*
RUN chmod +x /usr/local/bin/start-hadoop

VOLUME /opt/hdfs
EXPOSE 8020 9870 9864

# RUN apt-get update && apt-get install -y --no-install-recommends \
#     openjdk-8-jdk \
#     wget \
#     && rm -rf /var/lib/apt/lists/*
# RUN apt-get update && apt-get install -y --no-install-recommends \
#     openjdk-17-jdk \
#     wget \
#     && rm -rf /var/lib/apt/lists/*
# RUN apt-get update && apt-get install -y --no-install-recommends \
#     openjdk-11-jdk-headless \
#     wget \
#     && rm -rf /var/lib/apt/lists/*

# Download and extract Hadoop in a separate layer
# RUN wget -q https://downloads.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz -O /tmp/hadoop.tar.gz \
#     && mkdir -p ${HADOOP_HOME} \
#     && tar -xf /tmp/hadoop.tar.gz -C ${HADOOP_HOME} --strip-components=1 \
#     && rm /tmp/hadoop.tar.gz

# FROM ubuntu:latest
# FROM ubuntu:20.04

# # Copy installed Hadoop from base image
# COPY --from=hadoop-base ${HADOOP_HOME} ${HADOOP_HOME}

# # Set environment variables
# ENV HADOOP_VERSION=3.2.4
# ENV HADOOP_HOME=/opt/hadoop-${HADOOP_VERSION}
# ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
# # ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
# ENV HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
# ENV PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:/opt/util/bin

# # Install minimal runtime dependencies
# RUN apt-get update && apt-get install -y --no-install-recommends \
#     openjdk-8-jre-headless \
#     net-tools \
#     && rm -rf /var/lib/apt/lists/*
# # RUN apt-get update && apt-get install -y --no-install-recommends \
# #     openjdk-11-jre-headless \
# #     net-tools \
# #     && rm -rf /var/lib/apt/lists/*

# # Copy scripts and set permissions in one layer
# COPY start-hadoop*.sh /opt/util/bin/
# RUN chmod +x /opt/util/bin/start-hadoop* \
#     && sed -i 's/\r$//' /opt/util/bin/start-hadoop*

# VOLUME /opt/hdfs
# EXPOSE 8020 9870 9864