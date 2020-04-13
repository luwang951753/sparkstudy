package com.zysy.bigdata.sql.udaf

import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import org.apache.spark.sql.expressions.Aggregator

/**
  * 类型安全自定义聚合函数
  */
object TypeSafeMyAverageTest {

  /**
    *Data类存储读取的文件数据
    */
  case class Data(user_id: String, item_id: String, score: Double)
  //Average
  case class Average(var sum: Double,var count: Long)

  object SafeMyAverage extends Aggregator[Data, Average, Double] {
    override def zero: Average = Average(0.0D, 0L)

    override def reduce(b: Average, a: Data): Average = {
      b.sum += a.score
      b.count += 1L
      b
    }

    override def merge(b1: Average, b2: Average): Average = {
      b1.sum+=b2.sum
      b1.count+= b2.count
      b1
    }

    override def finish(reduction: Average): Double = reduction.sum / reduction.count

    override def bufferEncoder: Encoder[Average] = Encoders.product

    override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
  }

  def main(args: Array[String]): Unit = {
    //创建Spark SQL切入点
    val spark = SparkSession.builder().master("local").appName("My-Average").getOrCreate()
    //读取HDFS文件系统数据itemdata.data生成RDD
    val rdd = spark.sparkContext.textFile("hdfs://192.168.189.21:8020/input/mahout-demo/itemdata.data")
    //RDD转化成DataSet
    import spark.implicits._
    val dataDS =rdd.map(_.split(",")).map(d => Data(d(0), d(1), d(2).toDouble)).toDS()
    //自定义聚合函数
    val averageScore = SafeMyAverage.toColumn.name("average_score")
    dataDS.select(averageScore).show()
  }
}
