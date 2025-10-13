package com.example.scalaspark

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.ml.feature.{PCA, StandardScaler, Normalizer}
import org.apache.spark.ml.linalg.{Vector => MLVector, Vectors}
import com.johnsnowlabs.nlp.base.DocumentAssembler
import com.johnsnowlabs.nlp.annotator.{Tokenizer, SentenceDetector}
import com.johnsnowlabs.nlp.embeddings.XlmRoBertaEmbeddings
import com.johnsnowlabs.nlp.embeddings.SentenceEmbeddings
import com.johnsnowlabs.nlp.EmbeddingsFinisher

object Analysis {

  def calculateAverageNumberOfVacancies(data: DataFrame): DataFrame = {
    val result = data
      .groupBy(col("workplace_address").getField("municipality").as("municipality"))
      .agg(
        avg("number_of_vacancies").as("avg_number_of_vacancies"),
        count(lit(1)).as("num_vacancies_per_municipality")
      )

    val sortedResult = result.sort(col("num_vacancies_per_municipality").desc)
    sortedResult.write.option("header", "true").csv("output/municipal_vacancies")
    sortedResult
  }


  def clusterJobAds(
    /* 
    Cluster job ads with k-means and using PCA dimensionality reduction for better analysis and visualization:
      https://medium.com/codex/understanding-k-means-clustering-and-pca-unraveling-the-power-of-data-science-techniques-81c16b6b71f6 
    */
    data: DataFrame, 
    textCols: Seq[String] = Seq("headline", "description.company_information", "description.text", "description.needs", "description.requirements"),   // the text field use for clustering
    kRange: Range = 2 to 10,   // numbers of clusters to test
    numComponents: Int = 50,   // pca number of components k
    ): DataFrame = {

    // Text fields needed for clustering combined as only one column (with spaces in between)
      val combinedData = if (textCols.length > 1) {
      val combinedCol = concat_ws(" ", textCols.map(col): _*)
      data.withColumn("combined_text", combinedCol)
        } else {
          data.withColumn("combined_text", col(textCols.head))
        }

    // Pre-processing and cleaning
    val cleanTextUDF = udf((text: String) => {
    if (text == null) "" 
    else text.toLowerCase
            .replaceAll("\\p{Punct}", " ")    // replace punctuation with whitespace (in scala, \ itself needs to be escaped with \)
            .replaceAll("\\s+", " ")          // replace many whitespaces with just one whitespace
            .trim                 // remove leading and trailing whitespace
    })

    val cleanedData = withText.withColumn("cleaned_combined_text", cleanTextUDF(col("combined_text")))

    // Document Assembler
    val documentAssembler = new DocumentAssembler()
      .setInputCol("cleaned_combined_text")
      .setOutputCol("document")

    val tokenizer = new Tokenizer() 
      .setInputCols(Array("document"))
      .setOutputCol("token")

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
      embeddings,
      embeddingsFinisher
    ))

    val dataWithEmbeddings = pipeline.fit(cleanedData).transform(cleanedData)
    
    // val dataWithFeatures = dataWithEmbeddings.withColumn("features", vecify(col("finished_embeddings"))).na.drop(Seq("features"))

    // L2-normalization, essentially for removing effect of total word count
    val normalizer = new Normalizer()
    .setInputCol("finished_embeddings")
    .setOutputCol("features")
    .setP(2.0)

    val normalizedDF = normalizer.transform(dataWithEmbeddings)

    // PCA reduce dimensionality
    val pcaModel = new PCA()
    .setInputCol("features")
    .setOutputCol("pcaFeatures")
    .setK(numComponents)
    .fit(normalizedDF)

    val pcaDF = pcaModel.transform(normalizedDF)

    // cache because we'll train more KMeans models (speed up repeated access to intermediate data)
    pcaDF.cache()

    // find best k via silhouette method
    // Link: https://spark.apache.org/docs/latest/ml-clustering.html
    val evaluator = new ClusteringEvaluator()
      .setFeaturesCol("pcaFeatures")
      .setPredictionCol("prediction")     // prediction will be a cluster number between 0 and k
      .setMetricName("silhouette") 
    var bestK = kRange.head        // start with setting number of clusters to 2
    var bestScore = Double.NegativeInfinity     // this will be replaced by new best score
    var bestModel: KMeansModel = null
    val silhouetteScores = scala.collection.mutable.ArrayBuffer[(Int, Double)]()    // for storing progress

    for (k <- kRange) {
      val kmeans = new KMeans()
        .setK(k)
        .setFeaturesCol("pcaFeatures")
        .setPredictionCol("prediction") 
        .setSeed(1)       // seed for reproducibility
        .setMaxIter(50)    
        .setInitMode("k-means||")     // scalable k-means, instead of random initialization

      val model = kmeans.fit(pcaDF)
      val preds = model.transform(pcaDF)
      val score = evaluator.evaluate(preds)
      silhouetteScores += ((k, score))
      if (score > bestScore) {     // update best model (optimal number of clusters)
        bestScore = score
        bestK = k
        bestModel = model
      }
    }

    // Final clustering with best model after the iteration
    val finalDF = bestModel.transform(pcaDF).withColumnRenamed("prediction", "clusterNumber") 

    finalDF.write.mode("overwrite").option("header", "true").csv("output/job_clusters")

    val scoresDF = silhouetteScores.toSeq.toDF("k", "silhouette")
    scoresDF.write.mode("overwrite").option("header", "true").csv("output/job_clusters_silhouette")

    (finalDF, silhouetteScores.toSeq)
  }
}
