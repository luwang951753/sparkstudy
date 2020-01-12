package com.zysy.bigdata.structedstreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode

object SocketWordCount {

  def main(args: Array[String]): Unit = {
    // 1. 创建 SparkSession
    val spark = SparkSession.builder()
      .master("local[6]")
      .appName("socket_processor").config("spark.driver.host", "localhost")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    // 2. 读取外部数据源, 并转为 Dataset[String]
    val source = spark.readStream
      .format("socket")
      .option("host", "127.0.0.1")
      .option("port", 9999)
      .load()
      .as[String]

    // 3. 统计词频
    val words = source.flatMap(_.split(" "))
      .map((_, 1))
      .groupByKey(_._1)
      .count()

    // 4. 输出结果
    words.writeStream
      .outputMode(OutputMode.Complete())
      .format("console")
      .start()
      .awaitTermination()
  }


}
