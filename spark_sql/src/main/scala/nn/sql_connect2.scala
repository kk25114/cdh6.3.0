package nn

import java.util.Properties
import cn.SparkSql4.{connectionProperties, spark}
import org.apache.spark.sql.SparkSession

object sql_connect2 {
  def main(args: Array[String]) {
    //1创建session
    val spark: SparkSession = SparkSession.
      builder
      .appName("My Spark Application")
      .master("local[*]")
      .getOrCreate

    //配置数据库连接
    val connectionProperties = new Properties()
    connectionProperties.put("user", "root")
    connectionProperties.put("password", "123456")
    //    connectionProperties.put("driver", "com.mysql.jdbc.Driver")

    run1(spark)
    spark.stop()
  }


  private def run1(spark: SparkSession): Unit = {
    val studentDF = spark.read.jdbc("jdbc:mysql://10.214.22.46:3306/test_gkpj", "dw_km_cj_details", connectionProperties)

    studentDF.createTempView("dw_km_cj_details")


    val deptDF = spark.sql("select * from dw_km_cj_details")
    deptDF.show()

  }


}
