package com

object Demo2 {
  /**
    * 块表达式
    */
  def main(args: Array[String]): Unit = {
    val x = 0
    //在scala中{}中课包含一系列表达式，块中最后一个表达式的值就是块的值
    //下面就是一个块表达式
    val result = {
      if (x < 0) {
        -1
      } else if (x >= 1) {
        1
      } else {
        "error"
      }
    }
    println(result)

  }
}
