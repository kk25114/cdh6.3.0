package cn

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

object Sparksql_test {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()


    import spark.implicits._
    val df = spark.read.json("E:\\Mybigdata\\chd611\\spark_sql\\src\\main\\resources\\people.json")
    df.show()
    df.createOrReplaceTempView("person")
    spark.sql("select * from person").show()
    spark.stop()
  }
}
