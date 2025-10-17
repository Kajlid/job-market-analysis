import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, avg, count, lit, concat_ws, lower, regexp_replace, trim, split
from pyspark.ml.feature import PCA, Normalizer, Tokenizer, StopWordsRemover
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml import Pipeline
from pyspark.ml.feature import Tokenizer, HashingTF, IDF
from pyspark.sql.types import ArrayType, DoubleType, StringType
from pyspark.sql.window import Window

# Load environment variables
load_dotenv()

MONGO_USER = os.getenv("MONGO_USER")
MONGO_PASS = os.getenv("MONGO_PASS")
MONGO_HOST = os.getenv("MONGO_HOST")
MONGO_DB = os.getenv("MONGO_DB")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION")
HDFS_PATH = os.getenv("HDFS_PATH")

# Construct Mongo URI
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
    additional_stops = ["samt", "hos", "arbeta", "söker", "både", "vill", "kommer", "arbete", "jobb", "tjänsten", "arbetar", "ansökan", "får", "se", "enligt"]
    stops = english_stops + swedish_stops + additional_stops
    df_noStops = remover.setStopWords(stops).transform(df_tokens)

    # Find the most common words
    # Count top words globally
    global_keywords = (
    df_noStops
    .withColumn("top_words", F.explode(F.col("filtered_words")))
    .groupBy("top_words")
    .agg(F.count("*").alias("global_count"))  # 👈 renamed here
    .orderBy(F.desc("global_count")))

    # Keywords per kommun
    munici_keywords = (
    df_noStops
    .filter(col("workplace_address.municipality").isNotNull())
    .withColumn("top_words", F.explode(F.col("filtered_words")))
    .groupBy("workplace_address.municipality", "top_words")
    .agg(F.count("*").alias("municipality_count"))  # 👈 renamed here
    .orderBy("workplace_address.municipality", F.desc("municipality_count")))

    # Keywords per city
    city_keywords = (
    df_noStops
    .filter(col("workplace_address.city").isNotNull())
    .withColumn("top_words", F.explode(F.col("filtered_words")))
    .groupBy("workplace_address.city", "top_words")
    .agg(F.count("*").alias("city_count"))  # 👈 renamed here
    .orderBy("workplace_address.city", F.desc("city_count")))
    
    munici_keywords.write \
    .format("mongodb") \
    .mode("overwrite") \
    .option("uri", mongo_uri) \
    .option("database", MONGO_DB) \
    .option("collection", MONGO_COLLECTION).save() 

    return global_keywords 


def calculate_vacancies_per_municipality(dataframe):
    
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
    .option("collection", MONGO_COLLECTION).save()      # write to collection named "vacancies"


    # Also write CSV
    # result.write.option("header", True).mode("overwrite").csv("output/municipal_vacancies")

    return result


def cluster_job_ads_mongo_single_collection(dataframe, text_cols, k_range=range(2, 11), num_components=50):
    # Combine text columns
    combined_col = F.concat_ws(" ", *[F.col(c) for c in text_cols])
    df_combined = dataframe.withColumn("combined_text", combined_col)

    # Text processing pipeline (using TFIDF for encoding the text fields)
    tokenizer = Tokenizer(inputCol="combined_text", outputCol="words")
    hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=1000)
    idf = IDF(inputCol="rawFeatures", outputCol="features")
    pipeline = Pipeline(stages=[tokenizer, hashingTF, idf])
    model = pipeline.fit(df_combined)
    df_features = model.transform(df_combined)

    # Normalize
    normalizer = Normalizer(inputCol="features", outputCol="normFeatures")
    df_norm = normalizer.transform(df_features)

    # PCA, dimensionality reduction
    pca = PCA(k=num_components, inputCol="normFeatures", outputCol="pcaFeatures")
    pca_model = pca.fit(df_norm)
    df_pca = pca_model.transform(df_norm)

    # Find best K using silhouette
    evaluator = ClusteringEvaluator(featuresCol="pcaFeatures", predictionCol="prediction", metricName="silhouette")
    best_k, best_score, best_model = 2, float("-inf"), None

    for k in k_range:
        kmeans = KMeans(featuresCol="pcaFeatures", predictionCol="prediction", k=k, seed=1)
        model_k = kmeans.fit(df_pca)
        preds = model_k.transform(df_pca)
        score = evaluator.evaluate(preds)
        if score > best_score:
            best_k, best_score, best_model = k, score, model_k

    print(f"Best K: {best_k} with silhouette score: {best_score}")

    # Transform with best model
    final_df = best_model.transform(df_pca) \
        .withColumnRenamed("prediction", "clusterNumber") \
        .withColumn("job_title", F.col("occupation.label")) \
        .drop("features", "normFeatures", "pcaFeatures", "rawFeatures", "words", "combined_text")

    # Compute cluster-level summaries
    # Jobs per cluster
    jobs_per_cluster = final_df.groupBy("clusterNumber").count().withColumnRenamed("count", "jobs_in_cluster")

    # Most common city per cluster
    window = Window.partitionBy("clusterNumber").orderBy(F.desc("count"))
    common_city = final_df.groupBy("clusterNumber", "city") \
        .count() \
        .withColumn("rank", F.row_number().over(window)) \
        .filter(F.col("rank") == 1) \
        .select("clusterNumber", F.col("city").alias("most_common_city"))

    # Join summaries back to final_df
    final_df_with_summary = final_df.join(jobs_per_cluster, on="clusterNumber", how="left") \
                                    .join(common_city, on="clusterNumber", how="left") \
                                    .select("id", F.col("occupation.label").alias("job_title"),  F.col("city"),
                                            "clusterNumber", "most_common_city", "jobs_in_cluster")
                                    

    final_df_with_summary.write.format("mongodb") \
        .mode("overwrite") \
        .option("uri", mongo_uri) \
        .option("database", MONGO_DB) \
        .option("collection", MONGO_COLLECTION) \
        .save()


    return final_df_with_summary


# KMeans clustering on combined text fields
def cluster_job_ads(dataframe, text_cols, k_range=range(2, 11), num_components=50):
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
    .option("collection", MONGO_COLLECTION).save()      # write to collection named "clusters"
    
    # final_df.write.option("header", True).mode("overwrite").csv("output/job_clusters")

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
    
    # print(df.schema)
    # calculate_vacancies_per_municipality(df)
    
    # df.select("occupation.label").distinct().show(40, truncate=False)

    # df.filter(trim(col("occupation.label")) == "grundutbildad") \
    # .select("id", "headline", "occupation.label") \
    # .show(1, truncate=False)

    # cleanTextdf = calculate_keyword_frequencies(df)
    # cleanTextdf.select("words", "filtered_words").show(1, truncate=False)
    
    text_columns = ["headline", "description.text", "description.needs", "description.requirements"]
    clustered_df = cluster_job_ads(df, text_columns)
    # clustered_df = cluster_job_ads_mongo_single_collection(df, text_columns, k_range=range(2, 11), num_components=50)
    clustered_df.show(5, truncate=False)

    # avg_vacancies_df = calculate_avg_vacancies(df)
    # avg_vacancies_df.show(5, truncate=False)
    
    # calculate_keyword_frequencies(df)

    spark.stop()

if __name__ == "__main__":
    main()


