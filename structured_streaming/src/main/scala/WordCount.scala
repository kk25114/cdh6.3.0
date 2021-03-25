/**
 * nc -lk  9999
 * */




import org.apache.spark.sql.SparkSession

object WordCount {
  def main(args: Array[String]): Unit = {


    val spark = SparkSession
      .builder
      .master("local")
      .appName("StructuredNetworkWordCount")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._
    // 创建DataFrame
    // Create DataFrame representing the stream of input lines from connection to localhost:9999
    val lines = spark.readStream
      .format("socket")
      .option("host", "192.168.50.135")
      .option("port", 9999)
      .load()

    // Split the lines into words
    val words = lines.as[String].flatMap(_.split(" "))

    // Generate running word count
    val wordCounts = words.groupBy("value").count()

    // Start running the query that prints the running counts to the console
    // 三种模式：
    // 1 complete 所有内容都输出
    // 2 append   新增的行才输出
    // 3 update   更新的行才输出
    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()
  }
}