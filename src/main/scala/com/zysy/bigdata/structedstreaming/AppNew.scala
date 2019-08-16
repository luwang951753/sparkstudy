package com.zysy.bigdata.structedstreaming
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * @author lj
  * @createDate 2019/6/21 17:47
  **/
object AppNew {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("test").config("spark.driver.host", "localhost").
      master("local").getOrCreate()  // 创建一个 SparkSession 程序入口
    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")
    val lines = spark.readStream.textFile("D://work//ideaproject//sparkstudy//datas")  // 将 some_dir 里的内容创建为 Dataset/DataFrame；即 input table
    val words: Dataset[String] = lines.flatMap(_.split(","))
    val wordCounts: DataFrame = words.groupBy("value").count()
    //val wordCounts: Dataset[(String, Int)] = words.map(x => (x,1))
    // 对 "value" 列做 count，得到多行二列的 Dataset/DataFrame；即 result table

    val query = wordCounts.writeStream                 // 打算写出 wordCounts 这个 Dataset/DataFrame
      .outputMode("complete")                          // 打算写出 wordCounts 的全量数据
      .format("console")                               // 打算写出到控制台
      .start()

    query.awaitTermination()


    //TODO 窗口函数分组！！！！===============================
    import spark.implicits._

    //val words = ... // streaming DataFrame of schema { timestamp: Timestamp, word: String }

    // Group the data by window and word and compute the count of each group
    // Please note: we'll revise this example in <Structured Streaming 之 Watermark 解析>
    /*val windowedCounts = words.groupBy(
      Window($"timestamp", "10 minutes", "5 minutes"),
      $"word"
    ).count()*/

  }



}
