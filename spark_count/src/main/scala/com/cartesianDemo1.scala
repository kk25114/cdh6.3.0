package com
/*
笛卡尔积
*/

import org.apache.spark.{SparkConf, SparkContext}

object cartesianDemo1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("WordCountScala")
    conf.setMaster("local[4]") ;
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(Array("tom","tomas","tomasle","tomson"))
    val rdd2 = sc.parallelize(Array("1234","3456","5678","7890"))

    val rdd = rdd1.cartesian(rdd2);
    rdd.collect().foreach(t=>println(t))
  }
}


/*(tom,1234)
(tom,3456)
(tom,5678)
(tom,7890)
(tomas,1234)
(tomas,3456)
(tomas,5678)
(tomas,7890)
(tomasle,1234)
(tomasle,3456)
(tomasle,5678)
(tomasle,7890)
(tomson,1234)
(tomson,3456)
(tomson,5678)
(tomson,7890)*/