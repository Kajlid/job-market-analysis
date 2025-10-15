package example.scalaspark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.johnsnowlabs.nlp.base.DocumentAssembler
import com.johnsnowlabs.nlp.annotator.{SentenceDetector, Tokenizer, Normalizer, StopWordsCleaner}
import com.johnsnowlabs.nlp.embeddings.{XlmRoBertaEmbeddings, SentenceEmbeddings}
import com.johnsnowlabs.nlp.EmbeddingsFinisher
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import example.scalaspark.SparkMainBella.loadEmbeddings



object SparkClustering extends App {
  

  // ✅ Create Spark session with Spark NLP
  val spark = SparkSession.builder()
    .appName("SparkNLPJobAds")
    .master("local[*]")
    .config("spark.driver.memory", "8G")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.kryoserializer.buffer.max", "2000M")
    .getOrCreate()

  import spark.implicits._
  // ✅ Load data
  val path = s"hdfs://localhost:9000/user/isabella/jobstream/snapshot/yyyy=2025/mm=10/dd=07/job_ads.json"

  val embeddingsdf = loadEmbeddings(path, spark)
  embeddingsdf.printSchema()

  /*
  // Cluster variables
  val k = 5 // number of clusters
  val iterations = 20 // number of iterations
  
  // Traning the data
  val kmeans = new KMeans()
    .setK(k)
    .setSeed(1)
    .setFeaturesCol("finished_embeddings")
    .setPredictionCol("clusters")
  val kmeansModel = kmeans.fit(embeddingsdf)

  val predictions = kmeansModel.transform(embeddingsdf)

  val evaluator = new ClusteringEvaluator()
  val silhouette = evaluator.evaluate(predictions)
  println(f"✅ Silhouette with squared Euclidean distance = $silhouette%.4f")

  // ✅ Show some results
  predictions.select("id", "cluster").show(10, truncate = false)

  spark.stop()
  */
}
