import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, avg, count, lit, concat_ws, lower, regexp_replace, trim
from pyspark.ml.feature import PCA, Normalizer, Tokenizer, StopWordsRemover
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
HDFS_USER = os.getenv("HDFS_USER")
HDFS_PATH = os.getenv("HDFS_PATH")

# Construct Mongo URI
# mongo_uri = f"mongodb+srv://{MONGO_USER}:{MONGO_PASS}@{MONGO_HOST}/{MONGO_DB}.{MONGO_COLLECTION}"
mongo_uri = f"mongodb+srv://{MONGO_USER}:{MONGO_PASS}@{MONGO_HOST}/{MONGO_DB}?retryWrites=true&w=majority"


def calculate_keyword_frequencies(dataframe):

     # List of text columns to combine
    text_cols = [
        "headline",
        "description.conditions",
        "description.text",
        "occupation.label",
        "must_have.skills.label",
        "workplace_address.region"
    ]
    
     # Combine text columns into one string column
    combined_col = concat_ws(" ", *[col(c) for c in text_cols])
    combined_data = dataframe.withColumn("clean_text", lower(combined_col))
    
    # Clean text (remove HTML tags, non-letter chars, extra spaces)
    cleaned_data = (
        combined_data
        .withColumn("clean_text",
            regexp_replace(col("clean_text"), "<[^>]*>", "")  # remove HTML tags
        )
        .withColumn("clean_text",
            regexp_replace(col("clean_text"), "(?u)[^\\p{L}\\s]", " ")  # keep letters/spaces only
        )
        .withColumn("clean_text",
            regexp_replace(col("clean_text"), "\\s+", " ")  # collapse multiple spaces
        )
        .withColumn("clean_text", trim(col("clean_text")))  # trim leading/trailing spaces
    )
     # 3. Tokenize the text
    tokenizer = Tokenizer(inputCol="clean_text", outputCol="words")
    df_tokens = tokenizer.transform(cleaned_data)

    # 4. Remove stop words
    remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
    english_stops = StopWordsRemover.loadDefaultStopWords("english")
    swedish_stops = StopWordsRemover.loadDefaultStopWords("swedish")
    stops = english_stops + swedish_stops
    df_noStops = remover.setStopWords(stops).transform(df_tokens)

    # Find the 10 most common words
    # Explode the list of words into individual rows
    word_counts = (
        df_noStops
        .withColumn("word", F.explode(F.col("filtered_words")))
        .groupBy("word")
        .count()
        .orderBy(F.desc("count")))
    
    top10_words = [row["word"] for row in word_counts.limit(10).collect()]

    print("Top 10 most common words:", top10_words)
    

    
    return df_noStops


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

    # Load JSON data from HDFS
    data_path = HDFS_PATH
    df = spark.read.json(data_path)


    cleanTextdf = calculate_keyword_frequencies(df)
    cleanTextdf.select("words", "filtered_words").show(1, truncate=False)
    

    # avg_vacancies_df = calculate_avg_vacancies(df)
    # avg_vacancies_df.show(5, truncate=False)

    spark.stop()

if __name__ == "__main__":
    main()



# text_columns = ["headline", "description.company_information", "description.text",
#                 "description.needs", "description.requirements"]
# clustered_df = cluster_job_ads(df, text_columns)
# clustered_df.show(5, truncate=False)

