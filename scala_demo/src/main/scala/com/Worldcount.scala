package com

/**
  * 统计"hello tom hello jerry", "hello jerry", "hello kitty"中单词出现的个数
  */
object Worldcount {
  def main(args: Array[String]): Unit = {
    val lines = List("hello tom hello jerry", "hello jerry", "hello kitty")
    val stringToInt = lines.flatMap(_.split(" ")).map((_, 1)).groupBy(_._1).mapValues(_.foldLeft(0)(_+_._2))
    print(stringToInt)  //输出:Map(tom -> 1, kitty -> 1, jerry -> 2, hello -> 4)

//    val reverse = lines.flatMap(_.split(" ")).map((_, 1)).groupBy(_._1).map(t=>(t._1, t._2.size)).toList.sortBy(_._2).reverse
//    print(reverse)  //输出:List((hello,4), (jerry,2), (kitty,1), (tom,1))

  }
}
