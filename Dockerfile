# Use official OpenJDK image for PySpark
FROM openjdk:11-jre-slim

# Install Python & pip
RUN apt-get update && apt-get install -y python3 python3-pip curl && rm -rf /var/lib/apt/lists/*

# Install PySpark, MongoDB connector, and dotenv
RUN pip3 install pyspark==3.5.0 python-dotenv numpy pymongo

# Set working directory
WORKDIR /app

# Copy the script
COPY python_scripts/ python_scripts/

# Spark packages environment variable
ENV PYSPARK_SUBMIT_ARGS="--packages org.mongodb.spark:mongo-spark-connector_2.12:10.3.0 pyspark-shell"

# Command to run the script
CMD ["python3", "python_scripts/job_analysis.py"]
