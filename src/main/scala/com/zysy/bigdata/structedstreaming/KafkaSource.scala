package com.zysy.bigdata.structedstreaming

import org.apache.spark.sql.SparkSession

object KafkaSource {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[6]")
      .appName("kafka integration")
      .getOrCreate()

    import spark.implicits._

    val source = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "node01:9092,node02:9092,node03:9092")
      .option("subscribe", "streaming-bank")
      .option("startingOffsets", "earliest")
      .load()
      .selectExpr("CAST(value AS STRING)")
      .as[String]

    val result = source.map {
      item =>
        val arr = item.replace("\"", "").split(";")
        (arr(0).toInt, arr(1).toInt, arr(5).toInt)
    }
      .as[(Int, Int, Int)]
      .toDF("age", "job", "balance")

    result.writeStream
      .format("parquet") // 也可以是 "orc", "json", "csv" 等
      .option("path", "/dataset/streaming/result/")
      .start()
  }

}
