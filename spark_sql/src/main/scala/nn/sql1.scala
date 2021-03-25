package nn

import org.apache.spark.sql.SparkSession

object sql1 {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.
      builder
      .appName("My Spark Application")
      .master("local[*]")
      .getOrCreate

    val url = "jdbc:mysql://10.214.22.46:3306/test_gkpj?user=root&password=123456"

    val df = spark.read.format("jdbc").options(Map("url" -> url, "dbtable" -> "(SELECT * FROM dw_km_cj_details where examid=50")).load()

    println(df.count())
    println(df.rdd.partitions.size)
    import spark.sql
    df.createOrReplaceTempView("dw_km_cj_details")
    sql("select * from dw_km_cj_details").show()

  }

}
