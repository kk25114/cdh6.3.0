package cc

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


/**
 * 用户scala开发本地测试的spark wordcount程序
 */
object Reliability_partitionBy_mysql {
  def main(args: Array[String]): Unit = {
     /** 
      * 1.创建Spark的配置对象SparkConf，设置Spark程序的运行时的配置信息， 
      * 例如：通过setMaster来设置程序要链接的Spark集群的Master的URL,如果设置为local, 
      * 则代表Spark程序在本地运行，特别适合于机器配置条件非常差的情况。 
      */
     val spark: SparkSession = SparkSession.builder
       .appName("My Spark Application") // optional and will be autogenerated if not specified
       .master("local") // avoid hardcoding the deployment environment
       //.enableHiveSupport()              // self-explanatory, isn't it?
       //.config("spark.sql.warehouse.dir", "target/spark-warehouse")
       .getOrCreate

       
     import org.apache.log4j.Level
     spark.sparkContext.setLogLevel(Level.INFO.toString())
       
     val results = {
       spark.read.format("jdbc").option("driver", "com.mysql.jdbc.Driver")
         .option("url", "jdbc:mysql://10.8.4.168:3306/gk_test_data?useUnicode=true&characterEncoding=utf-8")
         .option("dbtable", "dm_km_cj_1k")
         .option("user", "root")
         .option("password", "123456")
         .option("fetchsize", "1000")
         .option("truncate", "true")
         .option("numPartitions", "100")
         .option("partitionColumn", "ssdm")
         .option("lowerBound", "1")
         .option("upperBound", "100")
         .load()
     }
     
     
     spark.sqlContext.setConf("spark.sql.shuffle.partitions","30")
     
     
    import org.apache.spark.sql.expressions.Window
     val rankSpec = Window.partitionBy("ssdm").orderBy("zcj")
 
    val test_data_1k_mutate = results.withColumn("perrank", percent_rank.over(rankSpec))
    
    val test_data_1k_mutate2 = test_data_1k_mutate.repartition(15)
//   val test_data_1k_mutate2 = test_data_1k_mutate.coalesce(300)
//     spark.sqlContext.setConf("spark.sql.shuffle.partitions","200")


     var test_data_1k_agg = {
       test_data_1k_mutate2.groupBy("ssdm").agg(
        skewness("zcj").alias("zcj_skewness"),
        kurtosis("zcj").alias("zcj_kurtosis"),
        count("perrank").alias("zcj_count"),
        var_pop("score1").alias("score1"),
        var_pop("score2").alias("score2"),
        var_pop("score3").alias("score3"),
        var_pop("score4").alias("score4"),
        var_pop("score5").alias("score5"),
        var_pop("score6").alias("score6"),
        var_pop("score7").alias("score7"),
        var_pop("score8").alias("score8"),
        var_pop("score9").alias("score9"),
        var_pop("score10").alias("score10"),
        var_pop("score11").alias("score11"),
        var_pop("score12").alias("score12"),
        var_pop("score13").alias("score13"),
        var_pop("score14").alias("score14"),
        var_pop("score15").alias("score15"),
        var_pop("score16").alias("score16"),
        var_pop("score17").alias("score17"),
        var_pop("score18").alias("score18"),
        var_pop("score19").alias("score19"),
        var_pop("score20").alias("score20"),
        var_pop("score21").alias("score21"),
        var_pop("score22").alias("score22"),
        var_pop("score23").alias("score23"),
        var_pop("score24").alias("score24"),
        var_pop("score25").alias("score25"),
        var_pop("score26").alias("score26"),
        var_pop("score27").alias("score27"),
        var_pop("score28").alias("score28"),
        var_pop("score29").alias("score29"),
        var_pop("score30").alias("score30"),
        var_pop("score31").alias("score31"),
        var_pop("score32").alias("score32"),
        var_pop("score33").alias("score33"),
        var_pop("score34").alias("score34"),
        var_pop("score35").alias("score35"),
        var_pop("score36").alias("score36"),
        var_pop("score37").alias("score37"),
        var_pop("score38").alias("score38"),
        var_pop("score39").alias("score39"),
        var_pop("score40").alias("score40"),
        var_pop("score41").alias("score41"),
        var_pop("score42").alias("score42"),
        var_pop("score43").alias("score43"),
        var_pop("score44").alias("score44"),
        var_pop("score45").alias("score45"),
        var_pop("score46").alias("score46"),
        var_pop("score47").alias("score47"),
        var_pop("score48").alias("score48"),
        var_pop("score49").alias("score49"),
        var_pop("score50").alias("score50"),
        var_pop("score51").alias("score51"),
        var_pop("score52").alias("score52"),
        var_pop("score53").alias("score53"),
        var_pop("score54").alias("score54"),
        var_pop("score55").alias("score55"),
        var_pop("score56").alias("score56"),
        var_pop("score57").alias("score57"),
        var_pop("score58").alias("score58"),
        var_pop("score59").alias("score59"),
        var_pop("score60").alias("score60"),
        var_pop("score61").alias("score61"),
        var_pop("score62").alias("score62"),
        var_pop("score63").alias("score63"),
        var_pop("score64").alias("score64"),
        var_pop("score65").alias("score65"),
        var_pop("score66").alias("score66"),
        var_pop("score67").alias("score67"),
        var_pop("score68").alias("score68"),
        var_pop("score69").alias("score69"),
        var_pop("score70").alias("score70"),
        var_pop("score71").alias("score71"),
        var_pop("score72").alias("score72"),
        var_pop("score73").alias("score73"),
        var_pop("score74").alias("score74"),
        var_pop("score75").alias("score75"),
        var_pop("score76").alias("score76"),
        var_pop("score77").alias("score77"),
        var_pop("score78").alias("score78"),
        var_pop("score79").alias("score79"),
        var_pop("score80").alias("score80"),
        var_pop("score81").alias("score81"),
        var_pop("score82").alias("score82"),
        var_pop("score83").alias("score83"),
        var_pop("score84").alias("score84"),
        var_pop("score85").alias("score85"),
        var_pop("score86").alias("score86"),
        var_pop("score87").alias("score87"),
        var_pop("score88").alias("score88"),
        var_pop("score89").alias("score89"),
        var_pop("score90").alias("score90"),
        var_pop("score91").alias("score91"),
        var_pop("score92").alias("score92"),
        var_pop("score93").alias("score93"),
        var_pop("score94").alias("score94"),
        var_pop("score95").alias("score95"),
        var_pop("score96").alias("score96"),
        var_pop("score97").alias("score97"),
        var_pop("score98").alias("score98"),
        var_pop("score99").alias("score99"),
        var_pop("score100").alias("score100")
       )
     }

     // results.cache()
     test_data_1k_agg.collect()
   }
}  