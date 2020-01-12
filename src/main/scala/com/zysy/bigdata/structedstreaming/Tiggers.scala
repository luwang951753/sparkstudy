package com.zysy.bigdata.structedstreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types.IntegerType

object Tiggers {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[6]")
      .appName("socket_processor")
      .getOrCreate()

    import org.apache.spark.sql.functions._
    import spark.implicits._

    spark.sparkContext.setLogLevel("ERROR")

    val source = spark.readStream
      .format("rate")
      .load()

    val result = source.select(log10('value) cast IntegerType as 'key, 'value)
      .groupBy('key)
      .agg(count('key) as 'count)
      .select('key, 'count)
      .where('key.isNotNull)
      .sort('key.asc)


    result.writeStream
      .outputMode(OutputMode.Complete())
      .trigger(Trigger.ProcessingTime("2 seconds"))
      .format("console")
      .start()
      .awaitTermination()
  }

}
