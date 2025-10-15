package example.scalaspark
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import com.johnsnowlabs.nlp.base.DocumentAssembler
import com.johnsnowlabs.nlp.annotator.{SentenceDetector, Tokenizer, Normalizer, StopWordsCleaner}
import com.johnsnowlabs.nlp.embeddings.{XlmRoBertaEmbeddings, SentenceEmbeddings}
import com.johnsnowlabs.nlp.EmbeddingsFinisher
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.ml.linalg.{Vector, Vectors, SparseVector}
import org.apache.spark.sql.functions.udf


object SparkMainBella extends App {

  def loadEmbeddings(path: String, spark: SparkSession):DataFrame = {

  import spark.implicits._
  
  val data = spark.read.json(path)

  // ✅ Combine text columns
  val textCols = Seq(
    "headline", 
    "description.conditions", 
    "description.text", 
    "occupation.label", 
    "must_have.skills.label", 
    "workplace_address.region"
  )
  val combinedCol = concat_ws(" ", textCols.map(col): _*)
  
  val combinedData = data.withColumn("clean_text", lower(combinedCol))
  
  val cleanedData = combinedData.withColumn(
  "clean_text",
  regexp_replace(col("clean_text"), "<[^>]*>", "")        // remove HTML tags
  )
  .withColumn("clean_text",
    regexp_replace(col("clean_text"), "(?u)[^\\p{L}\\s]", " ") // keep only letters and spaces
  )
  .withColumn("clean_text",
    regexp_replace(col("clean_text"), "\\s+", " ")             // collapse multiple spaces
  )
  .withColumn("clean_text", trim(col("clean_text")))


  // ✅ Spark NLP pipeline
  val documentAssembler = new DocumentAssembler()
    .setInputCol("clean_text")
    .setOutputCol("document")

  val tokenizer = new Tokenizer()
    .setInputCols("document")
    .setOutputCol("token")

  val normalizer = new Normalizer()
    .setInputCols("token")
    .setOutputCol("normalized")
    .setLowercase(true)
  
  val englishStops = StopWordsRemover.loadDefaultStopWords("english")
  val swedishStops = StopWordsRemover.loadDefaultStopWords("swedish")

  val combinedStops = (englishStops ++ swedishStops).distinct

  val stopWordsCleaner = new StopWordsCleaner()
    .setInputCols("normalized")
    .setOutputCol("cleanTokens")
    .setStopWords(combinedStops) // multilingual stopword support

  val embeddings = XlmRoBertaEmbeddings
    .pretrained("xlm_v_base", "xx")
    .setInputCols("document", "cleanTokens")
    .setOutputCol("embeddings")

  // neccessary to convert embeddings into a format usable by Spark ML
  val embeddingsFinisher = new EmbeddingsFinisher()
    .setInputCols("embeddings")
    .setOutputCols("finished_embeddings")
    .setOutputAsVector(true)
    .setCleanAnnotations(false)

  val pipeline = new Pipeline().setStages(Array(
    documentAssembler,
    tokenizer,
    normalizer,
    stopWordsCleaner,
    embeddings,
    embeddingsFinisher
  ))

  // ✅ Run pipeline
  val model = pipeline.fit(cleanedData)
  val result = model.transform(cleanedData)

  result.select("id", "clean_text", "finished_embeddings").show(2, truncate = false)
  //result.write.mode("overwrite").parquet("hdfs://localhost:9000/user/isabella/jobstream/processed/job_ads_with_embeddings.parquet") 
  


  // returning result
  result

}



}
