import org.apache.spark.{SparkConf, SparkContext}

/* done*/
object SimpleApp {
  def main(args: Array[String]) {
    //aliyun 47.112.134.68
//    val logFile = "hdfs://47.112.134.68:8020/user/hdfs/yanke_data/hello/word"
    val logFile = "hdfs://192.168.50.135:8020/user/hdfs/yanke_data/hello/word"
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)
    val rdd = sc.textFile(logFile)
    val wordcount = rdd.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

//    wordcount.saveAsTextFile("hdfs://47.112.134.68:8020/user/hdfs/yanke_data/hello/out1")
    wordcount.saveAsTextFile("hdfs://192.168.50.135:8020/user/hdfs/yanke_data/hello/out1")
    print("hello")
    sc.stop()

  }
}
