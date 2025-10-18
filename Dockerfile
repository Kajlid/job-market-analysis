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

# CMD ["/app/wait-for-hdfs.sh", "namenode", "8020", "/opt/spark/bin/spark-submit", "--master", "local[*]", "python_scripts/job_analysis.py"]
CMD ["/opt/spark/bin/spark-submit", "--master", "local[*]", "python_scripts/job_analysis.py"]
