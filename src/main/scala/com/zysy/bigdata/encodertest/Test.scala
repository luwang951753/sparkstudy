package com.zysy.bigdata.encodertest


import java.sql.Date
import java.time.LocalDate

import org.apache.spark.sql.SparkSession

object Test {
  implicit val toDate = (time:LocalDate) =>{
    Date.valueOf(time) //魔术发生在这里！
  }

  def main(args: Array[String]): Unit = {
    //implicit val fireServiceEncoder: org.apache.spark.sql.Encoder[newTest] = org.apache.spark.sql.Encoders.kryo[newTest]
    //implicit val fireServiceEncoder = org.apache.spark.sql.Encoders.kryo[LocalDate]

    val spark = SparkSession.builder().appName("test").config("spark.driver.host", "localhost").
      master("local").getOrCreate()  // 创建一个 SparkSession 程序入口
    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")

    val date =Seq(newTest(LocalDate.now()),newTest(LocalDate.now())).toDS()
    //val date =Seq(newTest(LocalDate.now()),newTest(LocalDate.now()))
    date.show()
  }



}
case class newTest(time:Date)
