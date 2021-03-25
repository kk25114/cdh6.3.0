import java.util.Properties

import org.apache.log4j.Level
import org.apache.spark.sql.functions.{col, count, sum}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

import scala.collection.mutable.ListBuffer


/*l连接本机的数据库,放在集群上需要指定数据库的id*/
object SparkSql3 {

  /**
   * 定义Item 结果类
   */
  case class Item(subjectId: Long, itemno: String, optionA: String, optionNumA: Long, optionB: String, optionNumB: Long, optionC: String, optionNumC: Long, optionD: String, optionNumD: Long, optionO: String, optionNumO: Long, rightOption: String, rightNum: Long, rightRatio: Double, dd: Double,optionType:Int)

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder.appName("My Spark Application").getOrCreate

    spark.sparkContext.setLogLevel(Level.ERROR.toString())

    spark.sqlContext.setConf("spark.sql.shuffle.partitions", "10")

    //配置数据库连接
    val connectionProperties = new Properties()
    connectionProperties.put("user", "root")
    connectionProperties.put("password", "root")
    connectionProperties.put("driver", "com.mysql.jdbc.Driver")

    //记录开始时间
    val begin = System.currentTimeMillis();

    //定义高分组临界分
    var highScore = 0d;

    //定义低分组临界分
    var lowScore = 0d;

    //定义结果Item集合
    var listResult = new ListBuffer[Item]

    /**
     * 计算每题数据到聚合表
     */
    def analysis(df: DataFrame, itemDF: DataFrame) {
      listResult = new ListBuffer[Item]
      val itemArray = itemDF.collect();
      itemArray.foreach(x => {
        val subjectId = x.getLong(0)
        val subjectName = x.get(1)
        val itemNo = x.getString(2)
        val allOptions = x.get(3)
        val rightOption = x.getString(4)
        val column = "option" + itemNo.toString()
        val columnNum = "num" + itemNo.toString()
        val optionType = x.getInt(6)
        val res = df.groupBy("subjectId", column).agg(count("ksh").name(columnNum)).sort("subjectId", column)
        var rightNum = res.where(col(column) === rightOption).collect()(0).getLong(2)
        val totalNum = res.agg(sum(columnNum)).collect()(0).getLong(0)
        val rightRatio = (rightNum * 1.0 / totalNum * 100)
        val dd = calItemDD(x, subjectId);
        val resArray = res.collect();
        var optionA = "A"
        var optionNumA = 0L
        var optionB = "B"
        var optionNumB = 0L
        var optionC = "C"
        var optionNumC = 0L
        var optionD = "D"
        var optionNumD = 0L
        var optionO = "#"
        var optionNumO = 0L
        resArray.foreach(x => {
          val option = x.get(1).toString()
          val optionNum = x.getLong(2)
          if (option == "A") {
            optionA = option;
            optionNumA = optionNum
          } else if (option == "B") {
            optionB = option;
            optionNumB = optionNum
          } else if (option == "C") {
            optionC = option;
            optionNumC = optionNum
          } else if (option == "D") {
            optionD = option;
            optionNumD = optionNum
          } else if (option == "#") {
            optionO = option;
            optionNumO = optionNum
          }
          val name = "option" + option;
        })
        listResult += new Item(subjectId, itemNo, optionA, optionNumA, optionB, optionNumB, optionC, optionNumC, optionD, optionNumD, optionO, optionNumO, rightOption, rightNum, rightRatio, dd,optionType)
      })
    }

    /*
     * 计算小题区分度
     */
    def calItemDD(x: Row, subjectId: Long): Double = {
      val itemNo = x.get(2)
      val column = "score" + itemNo.toString()
      val fullScore = x.getDouble(5)
      //高分组平均分
      val avg1 = spark.sql("SELECT AVG(" + column + ") FROM dw_km_cj_details WHERE examid=16 AND subjectid=" + subjectId + " AND zcj>=" + highScore).collect()(0).getDouble(0);
      //低分组平均分
      val avg2 = spark.sql("SELECT AVG(" + column + ") FROM dw_km_cj_details WHERE examid=16 AND subjectid=" + subjectId + " AND zcj<=" + lowScore).collect()(0).getDouble(0);
      //返回  小题区分度
      (avg1 - avg2) * 1.0 / fullScore
    }

    //细目表
    val itemDF = spark.read.jdbc("jdbc:mysql://192.168.1.128:3306", "test.dw_dim_item", connectionProperties)
    itemDF.createTempView("dw_dim_item")

    //科目表
    val subjectDF = spark.read.jdbc("jdbc:mysql://192.168.1.128:3306", "test.kn_subject", connectionProperties)
    subjectDF.createTempView("kn_subject")

    //成绩表DF
    var cjDF = spark.read.jdbc("jdbc:mysql://192.168.1.128:3306", "test.dw_km_cj_details", connectionProperties)
    cjDF.createTempView("dw_km_cj_details")

    //缓存成绩
    spark.catalog.cacheTable("dw_km_cj_details")

    //语文、英语科目
    val subjectids = Array(1L)

    //循环科目进行计算
    subjectids.foreach(subjectId => {

      cjDF = spark.sql("SELECT * FROM dw_km_cj_details WHERE examid=16 AND subjectid=" + subjectId + " AND isqk=0")

      // 获取高低分组临界分
      val scoreDF = spark.sql("SELECT percentile(zcj, 0.73) as highScore,percentile(zcj, 0.27) as lowScore FROM dw_km_cj_details WHERE examid=16 AND subjectid=" + subjectId)

      //获取高分组临界分
      highScore = scoreDF.head(1)(0).getDouble(0)

      //获取低分组临界分
      lowScore = scoreDF.head(1)(0).getDouble(1)

      //高分组成绩DF
      val highDF = spark.sql("SELECT * FROM dw_km_cj_details WHERE examid=16 AND subjectid=" + subjectId + " AND isqk=0 AND zcj>=" + highScore)

      //当前科目所有选项DF
      val item = spark.sql("SELECT s.id subjctid,s.NAME subjectname,i.itemNo,i.allOptions,i.rightOptioin,i.fullScore,i.optionType FROM dw_dim_item i INNER JOIN kn_subject s ON s.id=i.subjectId WHERE i.examId=16 and i.subjectid=" + subjectId)

      //导入包，支持List 转  DataFrame
      import spark.sqlContext.implicits._

      //【任务1】
      analysis(cjDF, item)

      //将结果集合转换为DataFrame
      val df = listResult.toDF
      df.show()
      //保存结果
      df.write.mode(SaveMode.Overwrite).jdbc("jdbc:mysql://192.168.1.128:3306", "test.dw_subject_result3", connectionProperties)

      //【任务2】 高分组所有小题
      analysis(highDF, item)
      //将结果集合转换为DataFrame
      val highDf = listResult.toDF
      highDf.show()
      //保存结果
      highDf.write.mode(SaveMode.Overwrite).jdbc("jdbc:mysql://192.168.1.128:3306", "test.dw_subject_result4", connectionProperties)
    })

    println("用时：" + (System.currentTimeMillis() - begin) / 1000 + "s")

    spark.stop()
  }

}
