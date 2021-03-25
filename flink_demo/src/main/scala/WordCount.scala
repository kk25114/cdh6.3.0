import org.apache.flink.api.scala.{AggregateDataSet, DataSet, ExecutionEnvironment}
import org.apache.flink.api.scala._

/**
 * 创建环境->读取数据->转换操作->输出
 *
 */
object WordCount {
  def main(args: Array[String]): Unit = {
    // 1.创建执行环境(上下文)
    val env = ExecutionEnvironment.getExecutionEnvironment

    // 2.从文件中读取数据
    val inputPath = "E:\\Mybigdata\\chd611\\flink_demo\\src\\main\\resources\\1.txt"
    val inputDS = env.readTextFile(inputPath)


  //进行wordcount处理,先分词,做flatmap转换成(word,1)二元组,最后聚合统计
  var wordCountDS = inputDS.flatMap(_.split(" ")).map((_, 1)).groupBy(0).sum(1)
  wordCountDS.print()

  }
}
