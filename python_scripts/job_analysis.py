import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, avg, count, lit, concat_ws, lower, regexp_replace, trim, split
from pyspark.ml.feature import PCA, Normalizer, Tokenizer, StopWordsRemover
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml import Pipeline
from pyspark.ml.feature import Tokenizer, HashingTF, IDF, Word2Vec
from pyspark.sql.types import ArrayType, DoubleType, StringType
from pyspark.sql.window import Window
import datetime
import json
import time
import requests
import subprocess
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError

mongo_user = "test-user"
mongo_pass = "test-password987654321"
# MONGO_HOST="cluster1.4hsuyrb.mongodb.net"
mongo_host="mongodb"
mongo_db="jobmarket"
mongo_port=27017

MONGO_COLLECTION="TEST"


# Construct Mongo URI
# For connecting to Atlas Cloud, the url would be:
# mongo_uri = f"mongodb+srv://{mongo_user}:{mongo_pass}@{mongo_host}:{mongo_port}/{mongo_db}?authSource=admin"
mongo_uri = f"mongodb://{mongo_user}:{mongo_pass}@{mongo_host}:{mongo_port}/{mongo_db}?authSource=admin"

retry_count = 0
while retry_count < 10:
    try:
        client = MongoClient(mongo_uri, serverSelectionTimeoutMS=2000)
        client.server_info()  # force connection
        print("MongoDB is ready")
        break
    except ServerSelectionTimeoutError:
        retry_count += 1
        print("Waiting for MongoDB to be ready...")
        time.sleep(2)
else:
    raise RuntimeError("MongoDB not reachable after 20s")

db = client[mongo_db]
print("MongoDB collections:", db.list_collection_names())      # check connectivity

def collect_data(spark):
    import tempfile
    import os

    jobstream_url = "https://jobstream.api.jobtechdev.se/snapshot"
    today = datetime.datetime.now(datetime.timezone.utc)

    # HDFS directory and file
    hdfs_dir = f"/user/hdfsuser/jobstream/snapshot/yyyy={today.year}/mm={today.month:02d}/dd={today.day:02d}/"
    hdfs_path = hdfs_dir + "job_ads.json"

    print(f"Fetching data from JobStream API...")
    headers = {"accept": "application/json"}
    params = {"limit": 100}
    # response = requests.get(jobstream_url, params=params, headers=headers, timeout=10)

    # if response.status_code != 200:
    #     raise RuntimeError(f"Failed to fetch jobstream snapshot: {response.status_code}")

    # data = response.json()
    # print(f"Fetched {len(data)} records")

    # Write JSON to a temporary local file
    # with tempfile.TemporaryDirectory() as tmpdir:
    #     local_file = os.path.join(tmpdir, "job_ads.json")
    #     with open(local_file, "w", encoding="utf-8") as f:
    #         for record in data:
    #             f.write(json.dumps(record) + "\n")  # line-delimited JSON

        # print(f"Saved snapshot to temporary file: {local_file}")

        # Ensure HDFS directory exists
        # fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
        # fs.mkdirs(spark._jvm.org.apache.hadoop.fs.Path(hdfs_dir))

        # # Write local file to HDFS
        # subprocess.run(["hdfs", "dfs", "-put", "-f", local_file, hdfs_path], check=True)
        # print(f"Snapshot stored in HDFS folder: {hdfs_dir}")
    
    with open("data/data.json") as f:
        data = json.load(f)  # list of dicts

    with open("data/job_ads_line.json", "w") as f:
        for record in data:
            f.write(json.dumps(record) + "\n")     # write it as separate lines
    
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    fs.mkdirs(spark._jvm.org.apache.hadoop.fs.Path(hdfs_dir))
    df = spark.read.json("file:///app/data/job_ads_line.json")    # read from local fs
    df.write.mode("overwrite").json(hdfs_path)         # read data into HDFS with Spark
    print(f"Snapshot stored in HDFS folder: {hdfs_dir}")

    df_hdfs = spark.read.json(hdfs_path)
    df_hdfs.show(5, truncate=False)

    return hdfs_path


def calculate_keyword_frequencies(dataframe, collection="keywords"):
    
    # Combine relevant text columns
    text_cols = [
        "headline",
        "description.conditions",
        "description.text",
        "occupation.label",
        "must_have.skills.label",
        "workplace_address.region"
    ]

    combined_col = concat_ws(" ", *[col(c) for c in text_cols])
    combined_data = dataframe.withColumn("clean_text", lower(combined_col))

    # Clean text (remove HTML, punctuation, etc.)
    cleaned_data = (
        combined_data
        .withColumn("clean_text", regexp_replace(col("clean_text"), "<[^>]*>", ""))
        .withColumn("clean_text", regexp_replace(col("clean_text"), "(?u)[^\\p{L}\\s]", " "))
        .withColumn("clean_text", regexp_replace(col("clean_text"), "\\s+", " "))
        .withColumn("clean_text", trim(col("clean_text")))
    )

    # Tokenize and remove stop words
    tokenizer = Tokenizer(inputCol="clean_text", outputCol="words")
    df_tokens = tokenizer.transform(cleaned_data)

    remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
    english_stops = StopWordsRemover.loadDefaultStopWords("english")
    swedish_stops = StopWordsRemover.loadDefaultStopWords("swedish")
    additional_stops = ["samt", "hos", "arbeta", "söker", "både", "vill", "kommer", "kommun", "län", "roll", "arbetsplats", "sök", "rekryteringsprocess",
                        "arbete", "jobb", "jobba", "tjänsten", "arbetar", "ansökan", "får", "se", "ser", "enligt", "del", "krav"]
    
    regions = [r[0].lower() for r in dataframe.select("workplace_address.region").distinct().collect() if r[0]]
    municipalities = [m[0].lower() for m in dataframe.select("workplace_address.municipality").distinct().collect() if m[0]]
    stops = english_stops + swedish_stops + additional_stops + regions + municipalities
    df_noStops = remover.setStopWords(stops).transform(df_tokens)

    # Extract municipality column and explode words
    words_df = (
        df_noStops
        .filter(col("workplace_address.municipality").isNotNull())
        .withColumn("municipality", col("workplace_address.municipality"))
        .withColumn("word", F.explode(F.col("filtered_words")))
    )
    # Count word frequencies per municipality
    word_counts = (
        words_df.groupBy("municipality", "word")
        .agg(F.count("*").alias("count"))
    )

    # Rank and select top 10 per municipality
    w = Window.partitionBy("municipality").orderBy(F.desc("count"))
    top10 = (
        word_counts.withColumn("rank", F.row_number().over(w))
        .filter(F.col("rank") <= 10)
    )

    # Aggregate top 10 into an array of {word, count} objects
    result = (
        top10.groupBy("municipality")
        .agg(F.collect_list(F.struct("word", "count")).alias("top_words"))
    )

    # Concatenates top 10 keywords to a string without counts
    result = result.withColumn(
        "top_10_keywords",
        F.concat_ws(", ", F.expr("transform(top_words, x -> x.word)"))
    )
    
    # Compute global top 20 keywords
    global_top = (
    df_noStops
    .withColumn("word", F.explode(F.col("filtered_words")))
    .groupBy("word")
    .agg(F.count("*").alias("count"))
    .orderBy(F.desc("count"))
    .limit(20)
    )
    
    # Convert the global top 20 keywords into a DataFrame
    global_top_array = (
        global_top.agg(F.collect_list(F.struct("word", "count")).alias("top_20_global"))
    )
    
    # Join this single-row DataFrame to all municipality rows
    final_result = result.crossJoin(global_top_array)
    
    # Save to MongoDB
    final_result.write \
        .format("mongodb") \
        .mode("overwrite") \
        .option("uri", mongo_uri) \
        .option("database", mongo_db) \
        .option("collection", collection) \
        .save()

    return final_result


def calculate_vacancies_per_municipality(dataframe, collection="vacancies"):
    
    # Filter out null values
    df_filtered = dataframe.filter(
        (col("workplace_address.municipality").isNotNull()) &
        (col("number_of_vacancies").isNotNull())
    )
    
    df_filtered = df_filtered.withColumn(
    "job_title", F.col("occupation.label")
    )
    
    df_filtered = df_filtered.withColumn(
    "job_title",
    trim(split(col("occupation.label"), ",")[0])    # only take first part of job title, e.g "Programmer" instead of "Programmer, senior"
    )

    # Most common job titles overall:
    most_common_titles = (
        df_filtered.groupBy("job_title")       # job title 
        .count()          # df will now be "job_title" and "count"
        .orderBy(F.desc("count"))    # sort 
        .limit(10)        # most 10 common job titles
        .collect()
    )
    
    most_common_titles_str = ", ".join([t["job_title"] for t in most_common_titles])
    
    df_filtered = df_filtered.withColumn(     # create a new column with the most common title
        "most_common_job_titles",        
        F.lit(most_common_titles_str).cast(StringType())      # add the literal value of most_common_titles to the df
    )
    
    df_filtered = df_filtered.withColumn(
    "municipality", F.col("workplace_address.municipality")
    )

    # Count number of job ads per municipality
    window_municipality = Window.partitionBy("municipality")
    df_filtered = df_filtered.withColumn(
        "num_job_ads_per_municipality",
        count("*").over(window_municipality)
    )

    # Find the most common job title per municipality
    job_counts_per_municipality = df_filtered.groupBy(
        "municipality", "job_title"
    ).count()
    
    rank_window = Window.partitionBy("municipality").orderBy(F.desc("count"))
    most_common_per_muni = (
        job_counts_per_municipality.withColumn("rank", F.row_number().over(rank_window))
        .filter(col("rank") == 1)
        .select(
            "municipality",
            col("job_title").alias("most_common_job_per_municipality")
        )
    )
    
    # Join back to df_filtered
    df_filtered = df_filtered.join(
        most_common_per_muni,
        on="municipality",
        how="left"         # left outer join to not lose any rows in df_filtered (the left table)
    )
    
    # Average number of vacancies per municipality:
    result = df_filtered.groupBy(col("municipality")) \
        .agg(
            avg("number_of_vacancies").alias("avg_number_of_vacancies"),
            count("*").alias("num_vacancies_per_municipality")    
        ) \
        .sort(col("num_vacancies_per_municipality").desc())

    # Join back to df_filtered
    result = df_filtered.join(
        result,
        on="municipality",
        how="left"         # left outer join to not lose any rows in df_filtered (the left table)
    ).select(
        "id",
        "job_title",
        "most_common_job_titles",
        "num_job_ads_per_municipality",
        "most_common_job_per_municipality",         
        "municipality",
        "avg_number_of_vacancies",
        "num_vacancies_per_municipality"
    )

    # Write to MongoDB
    result.write \
    .format("mongodb") \
    .mode("overwrite") \
    .option("uri", mongo_uri) \
    .option("database", mongo_db) \
    .option("collection", collection).save()      # write to collection named "vacancies"

    return result

def vacancies_over_time(df, collection="vacancies_over_time"):
    df = df.withColumn("date", F.to_date("publication_date"))
    result = df.groupBy("date") \
               .agg(count("*").alias("num_job_ads"),
                    avg("number_of_vacancies").alias("avg_vacancies"))
    result.write.format("mongodb").mode("overwrite") \
          .option("uri", mongo_uri) \
          .option("database", mongo_db) \
          .option("collection", collection).save()
    return result

def top_skills(df, collection="top_skills"):
    # Explode skills and remove nulls
    skills_df = df.withColumn("skill", F.explode("must_have.skills")) \
                  .filter(col("skill").isNotNull()) \
                  .withColumn("skill_label", col("skill.label")) \
                  .filter(col("skill_label").isNotNull())
    
    result = skills_df.groupBy("skill_label") \
                      .agg(count("*").alias("count")) \
                      .orderBy(F.desc("count")) \
                      .limit(50)
    
    result.write.format("mongodb").mode("overwrite") \
          .option("uri", mongo_uri).option("database", mongo_db).option("collection", collection).save()
    return result

def job_ads_per_region(df, collection="job_ads_per_region"):
    result = df.groupBy("workplace_address.region") \
               .agg(count("*").alias("num_jobs")) \
               .orderBy(F.desc("num_jobs"))
    result.write.format("mongodb").mode("overwrite") \
          .option("uri", mongo_uri).option("database", mongo_db).option("collection", collection).save()
    return result


def cluster_job_ads(dataframe, text_cols, k_range=range(2, 11), vector_size=30, num_components=20, collection="global_clusters"):
    """
    Cluster job ads using Word2Vec embeddings + PCA + KMeans
    Optimized for memory efficiency.
    """

    # Combine text columns
    combined_col = F.concat_ws(" ", *[col(c) for c in text_cols])
    df_combined = dataframe.withColumn("combined_text", combined_col)

    # Repartition to reduce memory pressure
    df_combined = df_combined.repartition(200)

    # Tokenizer and StopWordsRemover
    tokenizer = Tokenizer(inputCol="combined_text", outputCol="words")
    remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
    english_stops = StopWordsRemover.loadDefaultStopWords("english")
    swedish_stops = StopWordsRemover.loadDefaultStopWords("swedish")
    additional_stops = ["samt", "hos", "arbeta", "söker", "både", "vill", "kommer", "kommun", "län", "roll", "arbetsplats", "sök", "rekryteringsprocess",
                        "arbete", "jobb", "jobba", "tjänsten", "arbetar", "ansökan", "får", "se", "ser", "enligt", "del", "krav"]
    
    regions = [r[0].lower() for r in dataframe.select("workplace_address.region").distinct().collect() if r[0]]
    municipalities = [m[0].lower() for m in dataframe.select("workplace_address.municipality").distinct().collect() if m[0]]
    stops = english_stops + swedish_stops + additional_stops + regions + municipalities

    stopwords_remover = StopWordsRemover(inputCol="words", outputCol="filtered_words", stopWords=stops)

    # Word2Vec embeddings
    word2vec = Word2Vec(
        vectorSize=vector_size,
        minCount=2,
        maxSentenceLength=30,  # reduce memory usage
        inputCol="filtered_words",
        outputCol="features"
    )

    # Pipeline
    pipeline = Pipeline(stages=[tokenizer, stopwords_remover, word2vec])
    model = pipeline.fit(df_combined)
    df_features = model.transform(df_combined)

    # Unpersist intermediate DF to free memory
    df_combined.unpersist()

    # Normalize
    normalizer = Normalizer(inputCol="features", outputCol="normFeatures")
    df_norm = normalizer.transform(df_features)
    df_features.unpersist()

    # PCA
    pca = PCA(k=num_components, inputCol="normFeatures", outputCol="pcaFeatures")
    pca_model = pca.fit(df_norm)
    df_pca = pca_model.transform(df_norm).cache()  # cache only the final PCA result
    df_norm.unpersist()

    # Find best K using silhouette
    evaluator = ClusteringEvaluator(featuresCol="pcaFeatures", predictionCol="prediction", metricName="silhouette")
    # best_k, best_score, best_model = 2, float("-inf"), None

    # for k in k_range:
    #     kmeans = KMeans(featuresCol="pcaFeatures", predictionCol="prediction", k=k, seed=1)
    #     model_k = kmeans.fit(df_pca)
    #     preds = model_k.transform(df_pca)
    #     score = evaluator.evaluate(preds)
    #     if score > best_score:
    #         best_k, best_score, best_model = k, score, model_k
    
    kmeans = KMeans(featuresCol="pcaFeatures", predictionCol="prediction", k=6, seed=1)
    model_k = kmeans.fit(df_pca)
    preds = model_k.transform(df_pca)
    score = evaluator.evaluate(preds)

    # Transform to array for storage
    vector_to_array_udf = F.udf(lambda v: v.toArray().tolist(), ArrayType(DoubleType()))
    final_df = preds \
        .withColumnRenamed("prediction", "clusterNumber") \
        .withColumn("pcaFeaturesArray", vector_to_array_udf("pcaFeatures")) \
        .withColumn("silhouette_score", F.lit(score)) \
        .withColumn("job_title", col("occupation.label")) \
        .drop("features", "normFeatures", "pcaFeatures")

    # Unpersist PCA DF to free memory
    df_pca.unpersist()

    # Select only necessary columns to reduce memory
    final_df_selected_cols = final_df.select("id", "job_title", "clusterNumber", "silhouette_score")

    # Save to MongoDB
    final_df_selected_cols.write \
        .format("mongodb") \
        .mode("overwrite") \
        .option("uri", mongo_uri) \
        .option("database", mongo_db) \
        .option("collection", collection) \
        .save()

    return final_df_selected_cols


def main():
    print("Running the script")

    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("JobAnalysisApp").master("local[*]") \
        .config("spark.mongodb.write.connection.uri", mongo_uri) \
        .config("spark.mongodb.read.connection.uri", mongo_uri) \
        .config("spark.sql.caseSensitive", "true") \
        .config("spark.driver.memory", "48g") \
        .config("spark.executor.memory", "48g") \
        .config("spark.executor.memoryOverhead", "8g") \
        .config("spark.driver.memoryOverhead", "8g") \
        .config("spark.sql.files.maxPartitionBytes", "128MB") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020") \
        .config("spark.memory.offHeap.enabled", "true") \
        .config("spark.memory.offHeap.size", "16g") \
        .getOrCreate()
        
        # .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0") \
        # If java.lang.OutOfMemoryError:
        # .config("spark.memory.offHeap.enabled", "true") \
        # .config("spark.memory.offHeap.size", "16g") \
        # This will solve the problem with JVM heap size not being enough, but you need enough physical RAM.
            
    # Reduce shuffle size to avoid memory blow-up
    spark.conf.set("spark.sql.shuffle.partitions", 200)
        
    data_path = collect_data(spark)

    # Load JSON data from HDFS
    df = spark.read.json(data_path)
    
    print(df.schema)
    
    calculate_vacancies_per_municipality(df)
    
    calculate_keyword_frequencies(df)
    
    vacancies_over_time(df)
    
    top_skills(df)
    
    job_ads_per_region(df)
    
    text_columns = ["headline", "description.text"]
    
    cluster_job_ads(df, text_cols=text_columns)

    spark.stop()

if __name__ == "__main__":
    main()


