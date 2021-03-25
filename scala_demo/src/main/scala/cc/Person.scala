package cc


// 1. 声明类 和 getter／setter 方法
class Person {
  // name为变量，不设置初始值  _ 代表前面的name变量
  // var 会生成 getter和setter 方法
  var name: String = _
  // age为常量，不可变
  // val 只会生成 getter方法
  val age = 10
  // private 私有变量，只能在class内部使用
  private[this] val gender = "male"
}


// 2. 构造函数（ primary constructor & auxiliary constructor）
// 主构造方法  &  附属构造器
/*
1. 主构造器直接跟在类名后面， 主构造器中的参数，最后会被编译成字段
2. 主构造器执行的时候， 会执行类中的所有语句
3. 假设参数声明的时候不带val和var, name就相当于 private[this], 只能在class内部调用 !!!
*/
class Person2(var name: String, val age: Int) {
  // primary 初级的 constructor 构造器
  println("this is the primary constructor!")

  var gender: String = _
  val school: String = "ZJU"

  /*
  * 1. 附属构造器名称为this
  * 2. 每一个附属构造器必须首先调用已经存在的子构造器和附属构造器
  */
  def this(name: String, age: Int, gender: String) {
    this(name, age)
    this.gender = gender
  }
}


// 3. 继承（ extends ）/ 重写父类方法（ override def） / 重写字段（ override val / override var ）
// 不过： override var 测试发现编译不通过

class Student(name: String, age: Int, val major: String) extends Person2(name, age) {
  println("this is the subclass of Person, major is: " + major)

  override val school: String = "Bzz"

  // 子类覆盖父类的方法/变量 一定要用： override
  override def toString = "Override toString ..."
}

object Test11ScalaClass {
  def main(args: Array[String]) {

    val p = new Person //括号可省略
    p.name = "Jack"
    println(p.name + ": " + p.age)


    val p2 = new Person2("Jack", 20)
    println(p2.name + ":" + p2.age)


    val p3 = new Person2("Jack", 20, "male")
    println(p3.name + ":" + p3.age + ":" + p3.gender)


    val s = new Student("Jack", 20, "male")
    // 加载顺序： 先加载 父类，然后加载 子类
    println(s.name + ":" + s.age + ":" + s.major + ":" + s.school)
  }
}

