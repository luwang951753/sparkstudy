package com.zysy.bigdata.structedstreaming
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * @author lj
  * @createDate 2019/6/21 17:47
  **/
object App {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[4]").getOrCreate()  // 创建一个 SparkSession 程序入口
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

  }



}
