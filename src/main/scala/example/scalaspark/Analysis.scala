package com.example.scalaspark

import org.apache.spark.sql.DataFrame        // DataSet?
import org.apache.spark.sql.functions.{avg, col, count, lit}

object Analysis {
  def calculateAverageNumberOfVacancies(data: DataFrame): DataFrame = {
    val result = data
      .groupBy(col("workplace_address").getField("municipality").as("municipality"))
      .agg(
        avg("number_of_vacancies"),
        count(lit(1)).as("num_vacancies_per_municipality")
      )

    result.sort(col("num_vacancies_per_municipality").desc).write.option("header", "true").csv("output/municipal_vacancies")
    result
  }
}