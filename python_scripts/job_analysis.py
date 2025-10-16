import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, lit, concat_ws
from pyspark.ml.feature import PCA, Normalizer
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml import Pipeline

# Load environment variables
load_dotenv()

MONGO_USER = os.getenv("MONGO_USER")
MONGO_PASS = os.getenv("MONGO_PASS")
MONGO_HOST = os.getenv("MONGO_HOST")
MONGO_DB = os.getenv("MONGO_DB")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION")

# Construct Mongo URI
# mongo_uri = f"mongodb+srv://{MONGO_USER}:{MONGO_PASS}@{MONGO_HOST}/{MONGO_DB}.{MONGO_COLLECTION}"
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

# Average number of vacancies per municipality:
def calculate_avg_vacancies(dataframe):
    
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
    .option("collection", MONGO_COLLECTION) \
    .save()


    # Also write CSV
    # result.write.option("header", True).mode("overwrite").csv("output/municipal_vacancies")

    return result


# KMeans clustering on combined text fields
def cluster_job_ads(dataframe, text_cols, k_range=range(2, 11), num_components=50):
    # Combine text columns
    combined_col = concat_ws(" ", *[col(c) for c in text_cols])
    df_combined = dataframe.withColumn("combined_text", combined_col)

    # Vectorize using TF-IDF
    from pyspark.ml.feature import Tokenizer, HashingTF, IDF

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

    # Find best K using silhouette
    evaluator = ClusteringEvaluator(featuresCol="pcaFeatures", predictionCol="prediction", metricName="silhouette")
    best_k, best_score, best_model = 2, float("-inf"), None
    for k in k_range:
        kmeans = KMeans(featuresCol="pcaFeatures", predictionCol="prediction", k=k, seed=1)
        model = kmeans.fit(df_pca)
        preds = model.transform(df_pca)
        score = evaluator.evaluate(preds)
        if score > best_score:
            best_k, best_score, best_model = k, score, model

    final_df = best_model.transform(df_pca).withColumnRenamed("prediction", "clusterNumber")

    # Save as a CSV file
    final_df.write.option("header", True).mode("overwrite").csv("output/job_clusters")

    return final_df

avg_vacancies_df = calculate_avg_vacancies(df)
avg_vacancies_df.show(5, truncate=False)
# text_columns = ["headline", "description.company_information", "description.text",
#                 "description.needs", "description.requirements"]
# clustered_df = cluster_job_ads(df, text_columns)
# clustered_df.show(5, truncate=False)

# spark.stop()
