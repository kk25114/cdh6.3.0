//package cn
//
//import scala.actors.Actor
////类 MyActor
//class MyActor extends Actor{
//
//
//
//  override def act(): Unit = {
//    while (true) {
//      receive {
//        case "start" => {
//          println("starting ...")
//          Thread.sleep(1000)
//          println("started")
//        }
//        case "stop" => {
//          println("stopping ...")
//          Thread.sleep(1000)
//          println("stopped ...")
//        }
//      }
//    }
//  }
//}
//
//object MyActor {
//  def main(args: Array[String]) {
//    val actor = new MyActor
//    actor.start()
//    actor ! "start"
//    actor ! "stop"
//    println("消息发送完成！")
//  }
//
//}
