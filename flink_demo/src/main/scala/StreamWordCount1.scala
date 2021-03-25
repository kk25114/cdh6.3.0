import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object StreamWordCount1 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 接收socket文本流
    val textDstream: DataStream[String] = env.socketTextStream("192.168.50.170", 9999)

    // flatMap和Map需要引用的隐式转换
   // '\\s' 正则表达式 根据空格拆分字符串  如果不换行 aaa da  按空格切分计算
    //这句话好像是根据空格拆分字符串
    val dataStream: DataStream[(String, Int)] = textDstream.flatMap(_.split("\\s")).filter(_.nonEmpty).map((_, 1)).keyBy(0).sum(1)

    dataStream.print().setParallelism(1)

    // 启动executor，执行任务
    env.execute("Socket stream word count")
  }
}
