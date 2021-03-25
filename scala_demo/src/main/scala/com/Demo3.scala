package com

object Demo3 {
  /**
    * for循环表达式
    * @param args
    */
  def main(args: Array[String]): Unit = {
    //for(i <- 表达式),表达式1 to 10返回一个Range（区间）
    //每次循环将区间中的一个值赋给i
    for (i <- 1 to 10)
      println(i)  //输出1...10

    //for(i <- 数组)
    val arr = Array("a", "b", "c")
    for (i <- arr)
      println(i)  //打印abc

    //高级for循环
    //每个生成器都可以带一个条件，注意：if前面没有分号
    for(i <- 1 to 3; j <- 1 to 3 if i != j)
      print((10 * i + j) + " ")   //12 13 21 23 31 32 打印出i,j不相等的时候
    println()

    //for推导式：如果for循环的循环体以yield开始，则该循环会构建出一个集合
    //每次迭代生成集合中的一个值
    val v = for (i <- 1 to 10) yield i * 10
    println(v)  //Vector(10, 20, 30, 40, 50, 60, 70, 80, 90, 100)//遍历该集合可以取每一个数

  }
}
