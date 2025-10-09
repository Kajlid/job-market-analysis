package com.example.scalaspark

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{avg, col, count, lit, concat_ws}
import com.johnsnowlabs.nlp.base._
import com.johnsnowlabs.nlp.annotator._
import com.johnsnowlabs.nlp.embeddings.XlmRoBertaEmbeddings
import org.apache.spark.ml.Pipeline

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

  def createEmbeddingsForJobAds(data: DataFrame, textCols: Seq[String] = Seq("headline", "description", "employer")): DataFrame = {
    // needed for clustering
      val combinedData = if (textCols.length > 1) {
      val combinedCol = concat_ws(" ", textCols.map(col): _*)
      data.withColumn("combined_text", combinedCol)
        } else {
          data.withColumn("combined_text", col(textCols.head))
        }

      // Document Assembler
      val documentAssembler = new DocumentAssembler()
        .setInputCol("combined_text")
        .setOutputCol("document")

      // Embeddings
      val embeddings = XlmRoBertaEmbeddings.pretrained("xlm-roberta-base", "xx") // xx = multilingual
        .setInputCols("document")
        .setOutputCol("document_embeddings")

        val pipeline = new Pipeline().setStages(Array(
        documentAssembler,
        embeddings
      ))
    val model = pipeline.fit(combinedData)
    val dataWithEmbeddings = model.transform(combinedData)
    dataWithEmbeddings
  }

}
