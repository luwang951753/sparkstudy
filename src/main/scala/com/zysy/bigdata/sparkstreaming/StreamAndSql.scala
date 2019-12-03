package com.zysy.bigdata.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}

/**
  * @author lj
  * @createDate 2019/12/3 15:36
  **/
object StreamAndSql {

  def main(args: Array[String]): Unit = {
    val sparkConf=new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    /***
      * 创建StreamingContext需要sparkConf和batch interval
      */
    val ssc=new StreamingContext(sparkConf,Seconds(5))

    ssc.sparkContext.setLogLevel("ERROR")


    val lines = ssc.socketTextStream("192.168.1.163", 9999)
    val words = lines.flatMap(_.split(" "))


    // Convert RDDs of the words DStream to DataFrame and run SQL query

    //TODO  下面的只是一个batch中的数据，不是整体的！！！！
    words.foreachRDD {

      (rdd: RDD[String], time: Time) =>
      // Get the singleton instance of SparkSession
      val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)
      import spark.implicits._

      // Convert RDD[String] to RDD[case class] to DataFrame
      val wordsDataFrame = rdd.map(w => Record(w)).toDS()

      // Creates a temporary view using the DataFrame
      wordsDataFrame.createOrReplaceTempView("words")

      // Do word count on table using SQL and print it
      val wordCountsDataFrame =
        spark.sql("select word, count(*) as total from words group by word")
      println(s"========= $time =========")
      wordCountsDataFrame.show()
    }

    ssc.start()
    ssc.awaitTermination()
  }

  /** Case class for converting RDD to DataFrame */
  case class Record(word: String)

  /** Lazily instantiated singleton instance of SparkSession */
  object SparkSessionSingleton {

    @transient  private var instance: SparkSession = _

    def getInstance(sparkConf: SparkConf): SparkSession = {
      if (instance == null) {
        instance = SparkSession
          .builder
          .config(sparkConf)
          .getOrCreate()
      }
      instance
    }
  }


}
