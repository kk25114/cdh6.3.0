/**
 * nc -lk  9999
 * 集群模式运行
 * spark-submit \
 * --class WordCountSteaming \
 * --deploy-mode client \
 * --master yarn \
 * /opt/cloudera/parcels/CDH-6.1.1-1.cdh6.1.1.p0.875250/lib/spark/examples/jars/my_jar/test1.jar \
 *
 * 
 * */




import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

object WordCountSteaming2 {
  def main(args: Array[String]): Unit = {


    val spark = SparkSession
      .builder
      .master("local[2]")
      .appName("StructuredNetworkWordCount")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._
    // 创建DataFrame
    // Create DataFrame representing the stream of input lines from connection to localhost:9999
    val lines = spark.readStream
      .format("socket")
      .option("host", "192.168.50.170")
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
      .trigger(Trigger.ProcessingTime("1 seconds"))
      .start()

    query.awaitTermination()
  }
}