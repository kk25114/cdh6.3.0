import org.apache.spark
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object Spark_hive1 {


  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.
      builder
      .appName("My Spark Application")
      .enableHiveSupport()
      .config("spark.master","local[*]")
      .getOrCreate
      spark.sql("use yanke_data")
      val  df1 =spark.sql("select * from score")
      df1.show()

    //计算信度
    //mean均值，variance方差，stddev标准差，corr(Pearson相关系数)，skewness偏度，kurtosis峰度
    val df4=spark.sql("SELECT mean(age),variance(age),stddev(age),corr(age,yearsmarried),skewness(age),kurtosis(age) FROM score")

  }



  }
