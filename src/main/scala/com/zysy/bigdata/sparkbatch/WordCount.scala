package com.zysy.bigdata.sparkbatch

import com.zysy.bigdata.structedstreaming.Test.SparkConf
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author lj
  * @createDate 2019/12/18 16:16
  **/
object WordCount {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("HDFSWordCount")
    //TODO kryo是需要进行注册的！！1
    //conf.registerKryoClasses(Array(classOf[]))
    val session: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    var sc: SparkContext = session.sparkContext

    val lines = sc.textFile("D:\\work\\ideaproject\\sparkstudy\\datas\\data.txt")
    val words = lines.flatMap { _.split(",") }
    val pairs = words.map { word => (word, 1) }
    val wordCounts = pairs.reduceByKey(_ + _)

    wordCounts.checkpoint()
    print(wordCounts.toDebugString)
    wordCounts.foreach(print)

  }




}
