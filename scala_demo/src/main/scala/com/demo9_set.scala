package com

import scala.collection.immutable.HashSet

/**
  *immutable不可变的
  *extends app为主程序,同main函数
  */
object demo9_set extends  App{
  val set1 = new HashSet[Int]()
  //将元素和set1合并生成一个新的set，原有set不变
  val set2 = set1 + 4
  print(set1)
  //set中元素不能重复
  val set3 = set1 ++ Set(5, 6, 7)
  val set0 = Set(1,3,4) ++ set1
  println(set0.getClass)

}
