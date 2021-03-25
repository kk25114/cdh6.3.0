package com

object Demo4_method {
  /**
    * 方法函数的使用,  方法函数不同
    */

  //定义一个简单的方法 =后面是方法体,括号内是参数
  def m3(x:Int,y:Int):Int =x*y
  println(m3(3,4))  //12

  //定义一个方法(同java中,方法和函数有不同,函数是可以传递的)
  //方法m2参数要求是一个函数，函数的参数必须是两个Int类型
  //返回值类型也是Int类型
  def m1(f: (Int, Int) => Int) : Int = {
    f(2, 6)
  }


  //定义一个函数f1，参数是两个Int类型，返回值是一个Int类型
  val f1 = (x: Int, y: Int) => x + y
  //再定义一个函数f2
  val f2 = (m: Int, n: Int) => m * n

  //main方法
  def main(args: Array[String]) {

    //调用m1方法，并传入f1函数
    val r1 = m1(f1)
    println(r1)   //8 按f1函数

    //调用m1方法，并传入f2函数
    val r2 = m1(f2)
    println(r2)   //12
  }



}
