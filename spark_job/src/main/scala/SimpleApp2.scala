/**
 * clean -run main class-package
 * 带参数的在linux上执行
 * spark-submit \
 * --class SimpleApp2 \
 * --deploy-mode client \
 * --master yarn \
 * /opt/cloudera/parcels/CDH-6.1.1-1.cdh6.1.1.p0.875250/lib/spark/examples/jars/my_jar/count_args.jar \
 * hdfs://192.168.50.135:8020/user/hdfs/yanke_data/hello/word hdfs://192.168.50.135:8020/user/hdfs/yanke_data/hello/out2
 *
 * 一定要注意格式   '\'   代表每一类的结束
 *最后一行指定参数时不能换行
 *
 *
 */



import org.apache.spark.{SparkConf, SparkContext}

/* 放在集群上运行则去掉 setMaster()*/
object SimpleApp2 {
  def main(args: Array[String]) {
    //aliyun 47.112.134.68
    val logFile = args(0)
    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)

    val rdd = sc.textFile(logFile)
    val wordcount = rdd.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

        wordcount.saveAsTextFile(args(1))
    print("hello")
    sc.stop()

  }
}
