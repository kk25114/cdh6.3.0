package nn

import org.apache.spark.sql.SparkSession

import java.util.Properties

object sql_connect3 {
  def main(args: Array[String]) {
    //1创建session
    val spark: SparkSession = SparkSession.
      builder
      .appName("My Spark Application")
      .master("local[*]")
      .getOrCreate


    //2.配置数据库连接
    val connectionProperties = new Properties()
    connectionProperties.put("user", "root")
    connectionProperties.put("password", "123456")
    connectionProperties.put("driver", "com.mysql.jdbc.Driver")

    //3.取数据
//    val cjDF = spark.read.jdbc("jdbc:mysql://10.214.22.46:3306/test_gkpj", "dw_km_cj_details", connectionProperties).where("examid=50")
//    cjDF.createTempView("dw_km_cj_details")

//    val df1 = spark.sql("select count(1) from dw_km_cj_details ")
//    df1.show()

    //4.取报名信息
//    val bmkDF = spark.read.jdbc("jdbc:mysql://10.214.22.46:3306/test_gkpj", "dw_examstudent_fact", connectionProperties).where("examid=50")
//    bmkDF.createTempView("dw_examstudent_fact")
//    val df2 = spark.sql("select count(1) from dw_examstudent_fact")
//    df2.show()


    //5.从mysql读取数据
    val scoreDF = spark.read.jdbc("jdbc:mysql://10.214.22.46:3306/test_gkpj", "score", connectionProperties)
    scoreDF.createTempView("score")

    spark.sql("select count(1) from score").show
    spark.sql("select * from score").show

    //6.从hdfs读取数据
    val df1 = spark.read.csv("hdfs://10.214.22.47:8020/user/hdfs/yanke_data/data/test1.csv")
    df1.show()






    //5.计算信度
/*    Sx <- sum(var(x))
    SumSxi <- sum(apply(x,2,var))
    k <- ncol(x)
    alpha <- k/(k-1)*(1-SumSxi/Sx)
    return(alpha)*/





    spark.stop()
  }



}
