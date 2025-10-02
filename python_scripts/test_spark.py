from pyspark.sql import SparkSession
import os

spark = SparkSession.builder.appName("DF").config("spark.driver.memory", "4g").getOrCreate()

filename = f'/user/{os.getlogin()}/jobstream/snapshot/yyyy=2025/mm=09/dd=29/job_ads.json'

job_ads = spark.read.format("json").option("inferSchema", "true").option("header", "true").load(filename)

# job_ads.printSchema()

# job_ads.show(10)