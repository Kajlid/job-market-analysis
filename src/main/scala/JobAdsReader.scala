import org.apache.spark.sql.SparkSession

object JobAdsReader {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Read Job Ads from HDFS")
      .master("local[*]") // Use local mode with all cores
      .getOrCreate()

    // Path to your JSON file in HDFS
    val hdfsPath = "hdfs://localhost:9000/user/isabella/jobstream/snapshot/yyyy=2025/mm=10/dd=07/job_ads.json"

    // Read the JSON file
    val df = spark.read.json(hdfsPath)

    // Show the schema
    df.printSchema()

    // Optional: show a few rows
    df.show(1, truncate = false)

    spark.stop()
  }
}
