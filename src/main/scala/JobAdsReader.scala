import org.apache.spark.sql.{SparkSession, functions => F}
import org.apache.spark.ml.feature.{RegexTokenizer, StopWordsRemover}

object JobAdsReader {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Read and Clean Job Ads from HDFS")
      .master("local[*]") // local mode
      .config("spark.sql.session.charset", "UTF-8")
      .config("spark.executorEnv.LANG", "sv_SE.UTF-8")
      .config("spark.executorEnv.LC_ALL", "sv_SE.UTF-8")
      .getOrCreate()

    import spark.implicits._

    // 🗂 Path to your job ads JSON file in HDFS
    val hdfsPath = "hdfs://localhost:9000/user/isabella/jobstream/snapshot/yyyy=2025/mm=10/dd=07/job_ads.json"

    // 1️⃣ Read the job ads
    val df = spark.read.json(hdfsPath)

    // 2️⃣ Select ID and job description text
    val dfSelected = df.select($"id", $"description.text".as("text"))
      .filter($"text".isNotNull)

    // 3️⃣ Remove HTML tags and special characters
    var dfClean = dfSelected.withColumn(
      "clean_text",
      F.regexp_replace(F.col("text"), "<[^>]*>", "") // remove HTML
    )
    dfClean = dfClean.withColumn(
      "clean_text",
      F.regexp_replace(F.col("clean_text"), "(?u)[^\\p{L}\\s]", "")
    )

    // 4️⃣ Convert to lowercase
    dfClean = dfClean.withColumn("clean_text", F.lower(F.col("clean_text")))

    // 5️⃣ Tokenize words, splitting on non-letter characters 
    val tokenizer = new RegexTokenizer()
      .setInputCol("clean_text")
      .setOutputCol("words")
      .setPattern("[^\\p{L}]+")  // ✅ split on anything that is NOT a Unicode letter

    val dfTokenized = tokenizer.transform(dfClean)

    // 6️⃣ Remove Swedish + English stopwords
    val swedishStops = StopWordsRemover.loadDefaultStopWords("swedish")
    val englishStops = StopWordsRemover.loadDefaultStopWords("english")

    val combinedStops = swedishStops ++ englishStops

    val remover = new StopWordsRemover()
      .setInputCol("words")
      .setOutputCol("filtered_words")
      .setStopWords(combinedStops.distinct) // remove duplicates

    val dfKeywords = remover.transform(dfTokenized)

    // 7️⃣ Show example of cleaned keywords
    dfKeywords.select("id", "filtered_words")
      .show(1, truncate = false)

    spark.stop()
  }
}
