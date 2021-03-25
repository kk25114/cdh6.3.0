import org.apache.spark
import org.apache.spark.sql.SparkSession

object Spark_hive1 {


  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.
      builder
      .appName("My Spark Application")
      .enableHiveSupport()
      .getOrCreate

      spark.sql("select * from yanke_data.dept").show()

  }
}
