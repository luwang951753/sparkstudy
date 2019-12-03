package com.zysy.bigdata.sparkstreaming
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.hive.HiveContext

/**
 * @author Administrator
 */
object Top3HotProduct {
  
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
        .setMaster("local[*]")
        .setAppName("Top3HotProduct")
        .set("spark.driver.host", "localhost")
        .set("spark.sql.warehouse.dir", "D:\\work\\ideaproject\\sparkstudy\\spark-warehouse")

    val spark = new SparkSession.Builder().
      config(conf).
      //enableHiveSupport().
      getOrCreate()

    import spark.implicits._

    @transient
    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(1))
    sc.setLogLevel("error") //测试环境为了少打印点日志，我将日志级别设置为error
    
    val productClickLogsDStream = ssc.socketTextStream("192.168.1.163", 9999)
    val categoryProductPairsDStream = productClickLogsDStream
        .map { productClickLog => (productClickLog.split(" ")(2) + "_" + productClickLog.split(" ")(1), 1)}

    val categoryProductCountsDStream = categoryProductPairsDStream.reduceByKeyAndWindow(
        (v1: Int, v2: Int) => v1 + v2, 
        Seconds(60), 
        Seconds(7))

    //categoryProductCountsDStream.map()

    categoryProductCountsDStream.foreachRDD(
      categoryProductCountsRDD => {
        categoryProductCountsRDD.foreach(println)
        println("==============================================")
        println(System.currentTimeMillis()/1000)
      }
    )



    
//    categoryProductCountsDStream.foreachRDD(categoryProductCountsRDD => {
//      val categoryProductCountRowRDD = categoryProductCountsRDD.map(tuple => {
//        val category = tuple._1.split("_")(0)
//        val product = tuple._1.split("_")(1)
//        val count = tuple._2
//        Row(category, product, count)
//      })
//
//      val structType = StructType(Array(
//          StructField("category", StringType, true),
//          StructField("product", StringType, true),
//          StructField("click_count", IntegerType, true)))
//
//     // val hiveContext = new HiveContext(categoryProductCountsRDD.context)
//
//      val categoryProductCountDF = spark.createDataFrame(categoryProductCountRowRDD, structType)
//
//      categoryProductCountDF.createOrReplaceTempView("product_click_log")
//
//      val top3ProductDF = spark.sql(
//            "SELECT category,product,click_count "
//            + "FROM ("
//              + "SELECT "
//                + "category,"
//                + "product,"
//                + "click_count,"
//                + "row_number() OVER (PARTITION BY category ORDER BY click_count DESC) rank "
//              + "FROM product_click_log"
//            + ") tmp "
//            + "WHERE rank<=3")
//
//      top3ProductDF.show()
//    })
    
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
  
}