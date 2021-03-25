//package cn
//
///**
//  * Scala中的Actor能够实现并行编程的强大功能，
//  * 使用Scala能够更容易地实现多线程应用的开发
//  */
//
////注意导包是scala.actors.Actor
//import scala.actors.Actor
//
//object MyActor1 extends Actor {
//  //重新act方法
//  def act(){
//    for(i <- 1 to 10){
//      println("test " + i)
//      Thread.sleep(2000)
//    }
//  }
//}
//
//object MyActor2 extends Actor{
//  //重新act方法
//  def act(){
//    for(i <- 1 to 10){
//      println("test " + i)
//      Thread.sleep(2000)
//    }
//  }
//}
//
///**
//  * 说明：上面分别调用了两个单例对象的start()方法，
//  * 他们的act()方法会被执行，相同与在java中开启了两个线程，线程的run()方法会被执行
//  * 注意：这两个Actor是并行执行的，act()方法中的for循环执行完成后actor程序就退出了
//  */
//object ActorTest extends App{
//  //启动Actor
//  MyActor1.start()
//  MyActor2.start()
//
//  //控制台显示为两个一起显示
//
//}
//
