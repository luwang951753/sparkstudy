package com.zysy.bigdata.structedstreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType

object HDFSSource {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("hdfs_source")
      .master("local[6]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val userSchema = new StructType()
      .add("name", "string")
      .add("age", "integer")

    val source = spark
      .readStream
      .schema(userSchema)
      .json("hdfs://node01:8020/dataset/dataset")

    val result = source.distinct()

    result.writeStream
      .outputMode(OutputMode.Update())
      .format("console")
      .start()
      .awaitTermination()

  }

}
