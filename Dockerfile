FROM apache/spark:3.5.1

USER root

RUN apt-get update && \
    apt-get install -y python3-pip netcat curl iputils-ping && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy requirements first
COPY requirements.txt /app/

# Install all dependencies globally
RUN pip3 install --no-cache-dir -r requirements.txt

# Copy your code
COPY . /app

COPY wait-for-hdfs.sh /app/wait-for-hdfs.sh
RUN chmod +x /app/wait-for-hdfs.sh && \
    chmod +x /app/python_scripts/*.py && \
    chmod +x /opt/spark/bin/spark-submit

RUN sed -i 's/\r$//' /app/wait-for-hdfs.sh

# Ensure Spark uses this Python
ENV PYSPARK_PYTHON=/usr/bin/python3
ENV PYSPARK_DRIVER_PYTHON=/usr/bin/python3

# Add MongoDB Spark Connector and Java Driver dependencies
ADD https://repo1.maven.org/maven2/org/mongodb/spark/mongo-spark-connector_2.12/10.3.0/mongo-spark-connector_2.12-10.3.0.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-sync/4.11.1/mongodb-driver-sync-4.11.1.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/org/mongodb/bson/4.11.1/bson-4.11.1.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-core/4.11.1/mongodb-driver-core-4.11.1.jar /opt/spark/jars/

# CMD ["/app/wait-for-hdfs.sh", "namenode", "8020", "/opt/spark/bin/spark-submit", "--master", "local[*]", "python_scripts/job_analysis.py"]
CMD ["/opt/spark/bin/spark-submit", "--master", "local[*]", "python_scripts/job_analysis.py"]
