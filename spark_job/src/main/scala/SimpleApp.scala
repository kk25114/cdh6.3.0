/**
 * 在linux上执行
 * spark-submit \
 * --class SimpleApp2 \
 * --deploy-mode client \
 * --master yarn \
 * /opt/cloudera/parcels/CDH-6.1.1-1.cdh6.1.1.p0.875250/lib/spark/examples/jars/mys_jar/spark_count.jar \
 */

import org.apache.spark.{SparkConf, SparkContext}


/* 放在集群上运行则去掉 setMaster()*/
object SimpleApp {
  def main(args: Array[String]) {
    //aliyun 47.112.134.68
//    val logFile = "hdfs://192.168.50.135:8020/user/hdfs/yanke_data/hello/word"
    val logFile = "hdfs://10.214.22.47:8020/user/hdfs/yanke_data/hello/word"
    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)

    val rdd = sc.textFile(logFile)
    val wordcount = rdd.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

//    wordcount.saveAsTextFile("hdfs://192.168.50.135:8020/user/hdfs/yanke_data/hello/out1")
    wordcount.saveAsTextFile("hdfs://10.214.22.47:8020/user/hdfs/yanke_data/hello/out1")
    print("hello")
    sc.stop()

  }
}
