package com

object Demo1 {
  /**
    * 条件表达式
    * @param args
    */

  def main(args: Array[String]): Unit = {
    val x = 1
    //判断x的值，将结果赋给y
    val y = if (x > 0) 1 else -1
    //打印y的值
    println(y) //1

    //支持混合类型表达式
    val z = if (x > 1) 1 else "error"
    //打印z的值
    println(z) //error

    //如果缺失else，相当于if (x > 2) 1 else ()
    val m = if (x > 2) 1
    println(m)  //

    //在scala中每个表达式都有值，scala中有个Unit类，写做(),相当于Java中的void
    val n = if (x > 2) 1 else ()
    println(n)

    //if和else if
    val k = if (x < 0) 0
    else if (x >= 1) 1 else -1
    println(k)

  }
}
