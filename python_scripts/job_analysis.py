import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, avg, count, lit, concat_ws, lower, regexp_replace, trim, split
from pyspark.ml.feature import PCA, Normalizer, Tokenizer, StopWordsRemover
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml import Pipeline
from pyspark.sql.window import Window
from pyspark.ml.feature import Tokenizer, HashingTF, IDF
from pyspark.sql.types import ArrayType, DoubleType, StringType
import requests
import getpass
import os
import json
import subprocess
import datetime

# Load environment variables
load_dotenv()

MONGO_USER = os.getenv("MONGO_USER")
MONGO_PASS = os.getenv("MONGO_PASS")
MONGO_HOST = os.getenv("MONGO_HOST")
MONGO_DB = os.getenv("MONGO_DB")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION")
HDFS_USER = os.getenv("HDFS_USER")
HDFS_PATH = os.getenv("HDFS_PATH")

# Construct Mongo URI
mongo_uri = f"mongodb+srv://{MONGO_USER}:{MONGO_PASS}@{MONGO_HOST}/{MONGO_DB}?retryWrites=true&w=majority"

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("JobAnalysisApp").master("local[*]").config("spark.mongodb.write.connection.uri", mongo_uri) \
    .config("spark.sql.caseSensitive", "true") \
    .config("spark.driver.memory", "8g") \
    .config("spark.executor.memory", "8g") \
    .config("spark.sql.files.maxPartitionBytes", "128MB") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0") \
    .config("spark.mongodb.write.connection.uri", mongo_uri) \
    .getOrCreate()

# Load JSON data from HDFS
data_path = "hdfs://localhost:9000/user/root/jobstream/snapshot/yyyy=2025/mm=09/dd=29/job_ads.json"
df = spark.read.json(data_path)

def collect_data():
    # Configuration
    API_URL = "https://jobstream.api.jobtechdev.se/snapshot"
    HEADERS = {"accept": "application/json"}
    LOCAL_DIR = "data"
    LOCAL_FILE = os.path.join(LOCAL_DIR, "data.json")

    # Get current UTC date
    today = datetime.datetime.now(datetime.timezone.utc)
    date_path = f"yyyy={today.year}/mm={today.month:02d}/dd={today.day:02d}"

    # HDFS paths
    username = getpass.getuser()
    hdfs_dir = f"/user/{username}/jobstream/snapshot/{date_path}/"
    hdfs_file = hdfs_dir + "job_ads.json"

    # Fetch data from API
    response = requests.get(API_URL, headers=HEADERS)
    print("Status code:", response.status_code)
    data = response.json()

    # Save data locally 
    os.makedirs(LOCAL_DIR, exist_ok=True)
    with open(LOCAL_FILE, "w") as f:
        json.dump(data, f)
    print(f"Saved {len(data)} records to {LOCAL_FILE}")

    # Store data in HDFS
    subprocess.run(["hdfs", "dfs", "-mkdir", "-p", hdfs_dir])
    subprocess.run(["hdfs", "dfs", "-put", "-f", LOCAL_FILE, hdfs_file])
    print(f"Snapshot stored in HDFS: {hdfs_file}")
    
    return "hdfs://localhost:9000" + hdfs_file

def calculate_vacancies_per_municipality(dataframe, collection="vacancies_per_municipality"):
    
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
    .option("database", MONGO_DB) \
    .option("collection", collection).save()      # write to collection named "vacancies"

    return result

def calculate_keyword_frequencies(dataframe, collection="keywords_final"):
    
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
    additional_stops = ["samt", "hos", "arbeta", "söker", "både", "vill", "kommer",
                        "arbete", "jobb", "tjänsten", "arbetar", "ansökan", "får", "se", "enligt"]
    stops = english_stops + swedish_stops + additional_stops
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
    
    # Save to MongoDB Atlas
    final_result.write \
        .format("mongodb") \
        .mode("overwrite") \
        .option("uri", mongo_uri) \
        .option("database", MONGO_DB) \
        .option("collection", collection) \
        .save()

    return final_result

# Average number of vacancies per municipality:
def calculate_avg_vacancies(dataframe, collection="vacancies"):
    
    # Filter out null values
    df_filtered = dataframe.filter(
        (col("workplace_address.municipality").isNotNull()) &
        (col("number_of_vacancies").isNotNull())
    )
    result = df_filtered.groupBy(col("workplace_address.municipality").alias("municipality")) \
        .agg(
            avg("number_of_vacancies").alias("avg_number_of_vacancies"),
            count(lit(1)).alias("num_vacancies_per_municipality")
        ) \
        .sort(col("num_vacancies_per_municipality").desc())

    # Write to MongoDB
    result.write \
    .format("mongodb") \
    .mode("overwrite") \
    .option("uri", mongo_uri) \
    .option("database", MONGO_DB) \
    .option("collection", collection) \
    .save()

    return result


# KMeans clustering on combined text fields
def cluster_job_ads(dataframe, text_cols, k_range=range(2, 11), num_components=50, collection="clusters"):
    # Combine text columns
    combined_col = concat_ws(" ", *[col(c) for c in text_cols])
    df_combined = dataframe.withColumn("combined_text", combined_col)

    tokenizer = Tokenizer(inputCol="combined_text", outputCol="words")
    hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=1000)
    idf = IDF(inputCol="rawFeatures", outputCol="features")

    pipeline = Pipeline(stages=[tokenizer, hashingTF, idf])
    model = pipeline.fit(df_combined)
    df_features = model.transform(df_combined)

    # Normalize
    normalizer = Normalizer(inputCol="features", outputCol="normFeatures")
    df_norm = normalizer.transform(df_features)

    # PCA
    pca = PCA(k=num_components, inputCol="normFeatures", outputCol="pcaFeatures")
    pca_model = pca.fit(df_norm)
    df_pca = pca_model.transform(df_norm)

    # Find best K using the silhouette method
    evaluator = ClusteringEvaluator(featuresCol="pcaFeatures", predictionCol="prediction", metricName="silhouette")
    best_k, best_score, best_model = 2, float("-inf"), None
    for k in k_range:
        kmeans = KMeans(featuresCol="pcaFeatures", predictionCol="prediction", k=k, seed=1)
        model = kmeans.fit(df_pca)
        preds = model.transform(df_pca)
        score = evaluator.evaluate(preds)
        if score > best_score:
            best_k, best_score, best_model = k, score, model

    vector_to_array_udf = F.udf(lambda v: v.toArray().tolist(), ArrayType(DoubleType()))
    final_df = best_model.transform(df_pca).withColumnRenamed("prediction", "clusterNumber").withColumn("pcaFeaturesArray", vector_to_array_udf("pcaFeatures")) \
    .drop("features") \
    .drop("normFeatures") \
    .drop("pcaFeatures") 
    
    final_df = final_df.withColumn(
    "job_title", F.col("occupation.label")
    )
    
    final_df_selected_cols = final_df.select("id", "job_title", "clusterNumber")
    
    # Save as a CSV file
    final_df_selected_cols.write  \
    .format("mongodb") \
    .mode("overwrite") \
    .option("uri", mongo_uri) \
    .option("database", MONGO_DB) \
    .option("collection", collection).save()     
    

    return final_df



def main():

    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("JobAnalysisApp").master("local[*]").config("spark.mongodb.write.connection.uri", mongo_uri) \
        .config("spark.sql.caseSensitive", "true") \
        .config("spark.driver.memory", "8g") \
        .config("spark.executor.memory", "8g") \
        .config("spark.sql.files.maxPartitionBytes", "128MB") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0") \
        .config("spark.mongodb.write.connection.uri", mongo_uri) \
        .getOrCreate()

    # Write JSON data to HDFS
    data_path = collect_data()
    print(data_path)
    
    # Read data from HDFS
    df = spark.read.json(data_path)
    
    vacancies_per_municipality_df = calculate_vacancies_per_municipality
    
    keywords_df = calculate_keyword_frequencies(df)
    
    clusters_df = cluster_job_ads(df)

    spark.stop()

if __name__ == "__main__":
    main()
