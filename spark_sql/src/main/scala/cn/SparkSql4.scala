package cn

import org.apache.spark.sql.SparkSession
import java.util.Properties
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.Row
import org.apache.log4j.Level
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import java.util

import org.apache.spark.sql.types._
import org.apache.spark.sql.{ Row, SQLContext, SparkSession }
import scala.collection.mutable.HashMap

object SparkSql4 extends App {
  val spark: SparkSession = SparkSession.builder.appName("My Spark Application").master("local[*]").getOrCreate

  spark.sparkContext.setLogLevel(Level.ERROR.toString())

  spark.sqlContext.setConf("spark.sql.shuffle.partitions", "10")

  //配置数据库连接
  val connectionProperties = new Properties()
  connectionProperties.put("user", "root")
  connectionProperties.put("password", "root")
  connectionProperties.put("driver", "com.mysql.jdbc.Driver")

  //学生表
  val studentDF = spark.read.jdbc("jdbc:mysql://localhost:3306", "test.dw_examstudent_fact", connectionProperties)
  studentDF.createTempView("dw_examstudent_fact")

  //细目表
  val itemDF = spark.read.jdbc("jdbc:mysql://localhost:3306", "test.dw_dim_item", connectionProperties)
  itemDF.createTempView("dw_dim_item")

  //成绩表
  val cjDF = spark.read.jdbc("jdbc:mysql://localhost:3306", "test.dw_km_cj_details", connectionProperties)
  cjDF.createTempView("dw_km_cj_details")

  //科目表
  val subjectDF = spark.read.jdbc("jdbc:mysql://localhost:3306", "test.kn_subject", connectionProperties)
  subjectDF.createTempView("kn_subject")

  //试卷表
  val testpaperDF = spark.read.jdbc("jdbc:mysql://localhost:3306", "test.kn_testpaper", connectionProperties)
  testpaperDF.createTempView("kn_testpaper")

  //试卷明细表
  val sublistDF = spark.read.jdbc("jdbc:mysql://localhost:3306", "test.kn_testpaper_sublist", connectionProperties)
  sublistDF.createTempView("kn_testpaper_sublist")

  val itemArray = spark.sql("SELECT s.id subjctid,s.NAME subjectname,i.itemNo,i.allOptions,i.rightOptioin FROM dw_dim_item i INNER JOIN kn_subject s ON s.id=i.subjectId WHERE i.examId=16 AND i.optionType=1").collect()

  val b = HashMap[String, String]()

  val schema = StructType(List(
    StructField("itemNo", StringType, true),
    StructField("option", StringType, true),
    StructField("optionNum", StringType, true),
    StructField("rightOptionNum", StringType, true)))

  val dataList = new util.ArrayList[Row]()

  itemArray.foreach(x => {
    val subjectId = x.get(0)
    val subjectName = x.get(1)
    val itemNo = x.get(2)
    val allOptions = x.get(3)
    val rightOption = x.get(4)

//    val ANum = cjDF.filter("option" + itemNo + "='A'").count()
//    val BNum = cjDF.filter("option" + itemNo + "='B'").count()
//    val CNum = cjDF.filter("option" + itemNo + "='C'").count()
//    val DNum = cjDF.filter("option" + itemNo + "='D'").count()
//    val oNum = cjDF.filter("option" + itemNo + "='#'").count()
//    val rightNum = cjDF.filter("option" + itemNo + "='A'").count()

    dataList.add(Row(itemNo, "A", "", ""))

    //  cjDF.groupBy("option1").count().sort("option1").show()

    b.put(itemNo.toString(), rightOption.toString());

    //          spark.sql("SELECT " + itemNo + " itemNo,COUNT(*) num,option" + itemNo + " FROM dw_km_cj_details WHERE examid=16 AND subjectid=" + subjectId + "  GROUP BY option" + itemNo + ",score" + itemNo).show()
        cjDF.groupBy("subjectid", "option" + itemNo).agg(count("ksh").name("num")).sort("subjectId", "option" + itemNo).where(col("subjectid") === ("1") ).show

  })

  spark.createDataFrame(dataList, schema).show()

//  for ((key, value) <- b) {
//    println(key + ":" + value)
//  }

  var resSeq = Seq[Object]()
  resSeq = resSeq :+ Row("1", "2", "3")
  resSeq = resSeq :+ Row("4", "5", "6")

  var str = ""
  for ((key, value) <- b) {
    str += "(" + key + "," + value + "),"
  }

  val arr3 = Array("hadoop", "storm", "spark")

  val df = spark.createDataFrame(
    Seq(
      ("ming", 20, 15552211521L),
      ("hong", 19, 13287994007L),
      ("zhi", 21, 15552211523L))) toDF ("name", "age", "phone")

  //  df.show()

  //  cjDF.write.mode(SaveMode.Overwrite).jdbc("jdbc:mysql://localhost:3306", "test.dw_subject_result3", connectionProperties)

  spark.stop()

}

