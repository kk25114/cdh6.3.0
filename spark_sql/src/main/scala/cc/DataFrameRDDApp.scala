package cc

import org.apache.spark.sql.SparkSession

object DataFrameRDDApp {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("DataFrameRDDApp").master("local[2]").getOrCreate()

    //RDD===>DataFrame
    val rdd = spark.sparkContext.textFile("D:/hhc1.txt")
    //注意：需要带入隐式转换
    import spark.implicits._
    val studentDF = rdd.map(_.split(",")).map(line => Student(line(0), line(1))).toDF()
    studentDF.show()

    //将DataFrame转换成一张临时表，然后可以用SQL来操作
    studentDF.createOrReplaceTempView("student")
    spark.sql("select * from student").show();

    spark.stop()
  }

  case class Student(name1: String, name2: String)
}

// studentDF.show()输出为：
//+-----+-----+
//|name1|name2|
//+-----+-----+
//|   张三|   李四|
//|   王五|   赵六|
//+-----+-----+


// 小结：spark 有2种编程方式，一种是基于DataFrame API 一种是基于SQL的API（即，写SQL语句）

