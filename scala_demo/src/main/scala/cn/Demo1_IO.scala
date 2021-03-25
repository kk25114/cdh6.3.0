package cn

import java.io.{File, PrintWriter}

import scala.io.Source


//1.向文件中写入内容
object Demo1_IO {
  def main(args: Array[String]) {
    val writer = new PrintWriter(new File("d://test/test1.txt" ))
    writer.write("菜鸟教程")
    print("do")
    writer.close()
  }
}

//2.从屏幕上读取用户输入
object Demo2_IO {
  def main(args: Array[String]) {
    print("请输入 : " )
    val line = Console.readLine
    print("你输入的是:"+line)
  }
}

//3.从文件读取内容 写进去过能读取
object Demo3_IO {
  def main(args: Array[String]) {
    println("文件内容为:" )
    //读取的单行
    Source.fromFile("d://test/test2.txt" ).foreach{
      print
    }
  }
}





