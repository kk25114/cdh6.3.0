package nn
//dept 表有id deptname manager
//emp表有
import org.apache.spark.sql.SparkSession

object sql_connect {

  //定义为全局变量
  case class Dept(id:Int,deptname:String)


  def main(args: Array[String]) {
    //1创建session
    val spark: SparkSession = SparkSession.
      builder
      .appName("My Spark Application")
      .master("local[*]")
      .getOrCreate
    run1(spark)

    spark.stop()
  }



  private def run1(spark: SparkSession): Unit = {import spark.implicits._

    val jdbcDF1 = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://10.214.22.46:3306/test_gkpj")
      .option("dbtable", "dept")
      .option("user", "root")
      .option("password", "123456")
      .load()


    val jdbcDF2 = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://10.214.22.46:3306/test_gkpj")
      .option("dbtable", "emp")
      .option("user", "root")
      .option("password", "root")
      .load()

    //1.show
    //jdbcDF1.select("deptname").show()
    //jdbcDF1.show()

    //2.foreach get
    jdbcDF1.foreach{
      line =>
        val col1=line.getAs[Int]("id")
        //val col1=line.getAs[Int](0)

        val col2=line.getAs[String]("deptname")
        //val col2=line.getAs[String](1)
        print("foreach"+col1+col2)
    }

    //3.join查询
    jdbcDF1.createTempView("dept")
    jdbcDF2.createTempView("emp")
    print("---------多表之间的查询-------------")
    spark.sql("select * from dept a left join emp b on a.id=b.id").show()



    //4.中间结果保存为df
    val df1 = spark.sql("select * from dept limit 1")
    print("------------------------转换后的df-----------------------------")
    df1.show()






    //5.to df
    //a.定义全局变量class
    import spark.implicits._
    jdbcDF1.as[Dept]
    print("----------df转换为dataset----------")
    print(jdbcDF1)   //[id: int, deptname: string ... 1 more field]  转换为ds





  }


}
