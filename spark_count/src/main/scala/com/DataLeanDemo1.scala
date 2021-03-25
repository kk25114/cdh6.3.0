package com
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

/**
 *
 */
object DataLeanDemo1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("WordCountScala")
    conf.setMaster("local[4]") ;
    val sc = new SparkContext(conf)
    val rdd1 = sc.textFile("E:\\Mybigdata\\chd611\\spark_count\\src\\main\\resources\\test.txt",4)
    rdd1.flatMap(_.split(" ")).map((_,1)).map(t=>{
      val word = t._1
      val r = Random.nextInt(100)
      (word + "_" + r,1)
    }).reduceByKey(_ + _,4).map(t=>{
      val word = t._1;
      val count = t._2;
      val w = word.split("_")(0)
      (w,count)
    }).reduceByKey(_ + _,4).saveAsTextFile("E:\\Mybigdata\\chd611\\spark_count\\src\\main\\resources\\out1");
  }
}