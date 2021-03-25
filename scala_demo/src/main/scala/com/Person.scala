package com

class Person {
  //用val修饰的变量是只读属性，有getter但没有setter
  //（相当与Java中用final修饰的变量）
  val id = "9527"

  //用var修饰的变量既有getter又有setter
  var age: Int = 18

  //类私有字段,只能在类的内部使用
  private var name: String = "唐伯虎"

  //对象私有字段,访问权限更加严格的，Person类的方法只能访问到当前对象的字段
  private[this] val pet = "小强"

}
