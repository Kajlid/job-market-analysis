package com.example.scalaspark
import com.example.scalaspark.Analysis.calculateAverageNumberOfVacancies
import com.example.scalaspark.Analysis.clusterJobAds

object SparkMain extends App {
    val spark = createSparkSession("DF", isLocal = true)
    val path = s"hdfs://localhost:9000/user/root/jobstream/snapshot/yyyy=2025/mm=09/dd=29/job_ads.json"

    val data = spark.read.json(path)

    // val analysisResult = calculateAverageNumberOfVacancies(data = data)

    // analysisResult.show(truncate = false)

    val clusterResult = clusterJobAds(data = data)

    spark.stop()

}
