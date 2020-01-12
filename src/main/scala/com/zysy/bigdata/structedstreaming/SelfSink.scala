package com.zysy.bigdata.structedstreaming

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSinkProvider}
import org.apache.spark.sql.streaming.OutputMode

object SelfSink {

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

    class MySQLSink(options: Map[String, String], outputMode: OutputMode) extends Sink {

      override def addBatch(batchId: Long, data: DataFrame): Unit = {
        val userName = options.get("userName").orNull
        val password = options.get("password").orNull
        val table = options.get("table").orNull
        val jdbcUrl = options.get("jdbcUrl").orNull

        val properties = new Properties
        properties.setProperty("user", userName)
        properties.setProperty("password", password)

        data.write.mode(outputMode.toString).jdbc(jdbcUrl, table, properties)
      }
    }

    class MySQLStreamSinkProvider extends StreamSinkProvider with DataSourceRegister {

      override def createSink(sqlContext: SQLContext,
                              parameters: Map[String, String],
                              partitionColumns: Seq[String],
                              outputMode: OutputMode): Sink = {
        new MySQLSink(parameters, outputMode)
      }

      override def shortName(): String = "mysql"
    }

    result.writeStream
      .format("mysql")
      .option("username", "root")
      .option("password", "root")
      .option("table", "streaming-bank-result")
      .option("jdbcUrl", "jdbc:mysql://node01:3306/test")
      .start()
      .awaitTermination()
  }

}
