package cc

import org.apache.spark.sql.SparkSession

object DataFrameCase {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("DataFrameRDDApp").master("local[2]").getOrCreate()

    //RDD===>DataFrame
    val rdd = spark.sparkContext.textFile("D:/student.txt")
    //注意：需要带入隐式转换
    import spark.implicits._

    val studentDF = rdd.map(_.split("\\|")).map(line => Student(line(0).toInt, line(1), line(2), line(3))).toDF()
    //    studentDF.show(10)
    //    studentDF.take(10).foreach(println)
    //    studentDF.first()
    //    studentDF.head(10)
    studentDF.select("name", "email").show(10, false)
    //打印名字为空的
    studentDF.filter("name=''").show()
    studentDF.filter("name='' or name='null'").show()
    //排序
    studentDF.sort(studentDF("phone").desc).show();
    //多个列排序
    studentDF.sort("phone", "email").show();
    studentDF.sort(studentDF("phone").desc, studentDF("email").asc).show();
    //改列名
    studentDF.select(studentDF("name").as("student_name")).show()
    //join连接
    val studentDF2 = rdd.map(_.split("\\|")).map(line => Student(line(0).toInt, line(1), line(2), line(3))).toDF()
    studentDF.join(studentDF2, studentDF.col("id") === studentDF2.col("id")).show()
    
    spark.stop()
  }

  case class Student(id: Int, name: String, phone: String, email: String)
}