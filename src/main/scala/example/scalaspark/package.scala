package com.example

package object scalaspark {
    import org.apache.spark.sql.SparkSession
    
    def createSparkSession(appName: String, isLocal: Boolean): SparkSession = {
        if (isLocal) {
        SparkSession
            .builder()
            .config("spark.sql.caseSensitive", value = true)
            .config("spark.sql.session.timeZone", value = "UTC")
            .config("spark.driver.memory", value = "8G")
            .appName(appName)
            .master("local[*]")
            .getOrCreate()
        } else {
        SparkSession
            .builder()
            .config("spark.sql.caseSensitive", value = true)
            .config("spark.sql.session.timeZone", value = "UTC")
            .appName(appName)
            .getOrCreate()
        }

    }
  
}
