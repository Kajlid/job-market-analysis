# Job Market Analysis

This project provides a Dockerized data pipeline to fetch, process, and analyze job market data. It uses Apache Spark for data processing, HDFS for storage, and MongoDB for storing results. 

## Features

- Fetch job postings from JobStream API or local JSON files

- Store raw data in HDFS

- Analyze job postings:

    - Calculate keywords frequencies

    - Compute vacancies per municipality

    - Cluster job ads using Spark ML KMeans

- Store processed results in MongoDB



## Prerequisites

Make sure you have the following installed:

- Docker and Docker Compose
    - [Docker desktop](https://www.docker.com/products/docker-desktop/)
- Python 3.10+ if running the scripts locally
    -  Pip packages used listed in requirements.txt

For running the Python script


## Project Structure
```
├── docker-compose.yml
├── Dockerfile # Spark container
├── hdfs/ # HDFS Docker build context
│ ├── conf/
│ │ ├── core-site.xml
│ │ └── hdfs-site.xml
│ ├── Dockerfile
│ ├── start-hadoop-namenode.sh
│ ├── start-hadoop-datanode.sh
│ └── start-hadoop.sh
├── mongo-init.js # MongoDB initialization script
├── python_scripts/
│ ├── job_analysis.py # Main Spark job
│ └── ... # Other Python scripts
├── requirements.txt
├── data/ # Local JSON snapshots (optional)
└── output/ # Optional output CSV or logs
```


## Project Setup

1. Clone the repository:

```bash
git clone https://github.com/Kajlid/job-market-analysis.git
cd job-market-analysis
```

2. Set up and run with Docker:
```bash
docker-compose up --build
```
- MongoDB: listens on port 27017
- HDFS NameNode: RPC port 8020, Web UI 9870
- HDFS DataNode: port 9864

3. Connect to Mongo shell and access the data:
```bash
mongosh "mongodb://test-user:test-password987654321@127.0.0.1:27017/jobmarket?authSource=admin"
```
Alternatively, download [MongoDB Compass](https://www.mongodb.com/try/download/compass) and connect to the url to see the collections in a GUI. 

## HDFS Web UI

NameNode Web UI: http://localhost:9870

Browse directories and uploaded JSON snapshots.

## Troubleshooting

| Issue                                         | Solution                                                                                     |
|-----------------------------------------------|---------------------------------------------------------------------------------------------|
| MongoDB: Missing configuration for: collection | Make sure `MONGO_COLLECTION` variable is set                                                |
| HDFS: PATH_NOT_FOUND                           | Ensure the directory exists or let Spark create it using `_jvm.org.apache.hadoop.fs.FileSystem` |
| Spark Mongo Connector: NoClassDefFoundError   | Ensure the jar is added in Dockerfile to `/opt/spark/jars/`                                  |
| java.lang.OutOfMemoryError                                           | Increase driver/executor memory in SparkSession or Docker resources. Free up disk/memory if needed.


## Contact
This project was a part of the course ID2221 Data-Intensive Computing.

For questions, you can contact us here or create an issue: \
Kajsa Lidin: kajsalid@kth.se   \
Isabella Gobl: igobl@kth.se

