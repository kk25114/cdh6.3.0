import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    //sparkContext
    val conf = new SparkConf();
    conf.setAppName("WordCount");
    //设置master属性
//        conf.setMaster("spark://192.168.50.135:7077") ;    //thirft server  cdh暂无法使用
      conf.setMaster("local[1]") ;
    //通过conf创建sc
    val sc = new SparkContext(conf);

    //a.加载文本文件
//        val logFile = "E:\\Mybigdata\\chd611\\spark_count\\src\\main\\resources\\word.txt"
    //b.加载hdfs文件
     val logFile = "hdfs://192.168.50.135:8020/user/hdfs/yanke_data/hello/word"
    // val logFile = "hdfs://47.112.134.68:8020/user/hdfs/yanke_data/hello/word"
    val rdd1 = sc.textFile(logFile);
    //压扁
    val rdd2 = rdd1.flatMap(line => line.split(" ")) ;
    //映射w => (w,1)
    val rdd3 = rdd2.map((_,1))
    val rdd4 = rdd3.reduceByKey(_ + _)
    val r = rdd4.collect()
    r.foreach(println)
  }
}
