package com.example.scalaspark
import com.example.scalaspark.Analysis.calculateAverageNumberOfVacancies
import com.example.scalaspark.Analysis.clusterJobAds
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{regexp_replace => F_regexp_replace}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.ml.feature.RegexTokenizer
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.ml.feature.{PCA, StandardScaler, Normalizer}
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import com.johnsnowlabs.nlp.base.DocumentAssembler
import com.johnsnowlabs.nlp.annotator.{Tokenizer, SentenceDetector}
import com.johnsnowlabs.nlp.embeddings.XlmRoBertaEmbeddings
import com.johnsnowlabs.nlp.embeddings.SentenceEmbeddings
import com.johnsnowlabs.nlp.EmbeddingsFinisher
import org.apache.spark.ml.Pipeline

object SparkMain extends App {
    val spark = createSparkSession("DF", isLocal = true)
    val path = s"hdfs://localhost:9000/user/root/jobstream/snapshot/yyyy=2025/mm=09/dd=29/job_ads.json"

    val data = spark.read.json(path)

    // val analysisResult = calculateAverageNumberOfVacancies(data = data)

    // analysisResult.show(truncate = false)

    val textCols = Seq("headline", "description.conditions", "description.text", "occupation.label", "must_have.skills.label", "workplace_address.region")

    val combinedData = if (textCols.length > 1) {
      val combinedCol = concat_ws(" ", textCols.map(col): _*)
      data.withColumn("combined_text", combinedCol)
        } else {
          data.withColumn("combined_text", col(textCols.head))
        }

    val cleanTextUDF = udf((text: String) => {
    if (text == null) "" 
    else text.toLowerCase
            .replaceAll("\\p{Punct}", " ")    // replace punctuation with whitespace (in scala, \ itself needs to be escaped with \)
            .replaceAll("\\s+", " ")          // replace many whitespaces with just one whitespace
            .trim                 // remove leading and trailing whitespace
    })

    var dfClean = combinedData.withColumn(
      "clean_text",
      F_regexp_replace(col("combined_text"), "<[^>]*>", "") // remove HTML
    )
    dfClean = dfClean.withColumn(
      "clean_text",
      F_regexp_replace(col("clean_text"), "(?u)[^\\p{L}\\s]", "") // remove special characters
    )

    // this is the final cleaned combined text column
    val cleanedData = dfClean.withColumn("clean_text", cleanTextUDF(col("combined_text")))

    // Tokenize words, splitting on non-letter characters 
    val tokenizer = new RegexTokenizer()
      .setInputCol("clean_text")
      .setOutputCol("words")
      .setPattern("[^\\p{L}]+")

    val dfTokenized = tokenizer.transform(cleanedData)

    val swedishStops = StopWordsRemover.loadDefaultStopWords("swedish")
    val englishStops = StopWordsRemover.loadDefaultStopWords("english")

    val combinedStops = swedishStops ++ englishStops

    val remover = new StopWordsRemover()
      .setInputCol("words")
      .setOutputCol("filtered_words")
      .setStopWords(combinedStops.distinct) // remove duplicates

    val dfKeywords = remover.transform(dfTokenized)

    // Document Assembler
    val documentAssembler = new DocumentAssembler()
      .setInputCol("cleaned_combined_text")
      .setOutputCol("document")

    // Embeddings (XlmRoBerta is used because of multilingual support)
    // Consider SentenceEmbeddings layer on top of token embeddings to reduce downstream size.
    val embeddings = XlmRoBertaEmbeddings.pretrained("xlm_v_base","xx")     // xx for multilingual
    .setInputCols(Array("document", "token")) 
    .setOutputCol("embeddings") 

    // convert annotations into a vector that Spark ML can use
    val embeddingsFinisher = new EmbeddingsFinisher()
      .setInputCols("embeddings")
      .setOutputCols("finished_embeddings")     
      .setOutputAsVector(true) 
      .setCleanAnnotations(false)

    val pipeline = new Pipeline().setStages(Array(
      documentAssembler,
      tokenizer,
      remover,
      embeddings,
      embeddingsFinisher
    ))

    val dataWithEmbeddings = pipeline.fit(cleanedData).transform(cleanedData)

    dataWithEmbeddings.select("id", "filtered_words", "finished_embeddings")
      .show(1, truncate = false)

    // val clusterResult = clusterJobAds(data = data)

    // dfKeywords.select("id", "filtered_words")
    //   .show(1, truncate = false)

    spark.stop()

}
