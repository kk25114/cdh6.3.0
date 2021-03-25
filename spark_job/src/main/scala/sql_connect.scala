/**
 *spark-submit --class sql_connect --deploy-mode client --master yarn /opt/cloudera/parcels/CDH-6.1.1-1.cdh6.1.1.p0.875250/lib/spark/examples/jars/my_jar/spark_job-1.0-SNAPSHOT.jar
 *
 * */







import org.apache.spark.sql.SparkSession

object sql_connect {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL  example")
      .getOrCreate()
    run1(spark)

    spark.stop()
  }

  private def run1(spark: SparkSession): Unit = {

    val jdbcDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://192.168.1.128:3306/mytest")
      .option("dbtable", "dept")
      .option("user", "root")
      .option("password", "root")
      .load()

//      jdbcDF.show()

//    jdbcDF.select("deptname").show()
    jdbcDF.createOrReplaceTempView("dept")

    val deptDF = spark.sql("select * from dept")
    deptDF.show()


  }






}
