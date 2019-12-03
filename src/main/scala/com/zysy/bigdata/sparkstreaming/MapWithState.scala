package com.zysy.bigdata.sparkstreaming

import kafka.serializer.StringDecoder
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.api.java.JavaPairInputDStream
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author lj
  * @createDate 2019/12/3 16:19
  **/
object MapWithState {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("StreamingMapWithState")
      .setMaster("local[*]")
    val sc = SparkContext.getOrCreate(conf)
    val ssc = new StreamingContext(sc, Seconds(1))
    ssc.sparkContext.setLogLevel("ERROR")
    // 当调用updateStateByKey函数API的时候，必须给定checkpoint dir
    // 路径对应的文件夹不能存在
    ssc.checkpoint("hdfs://192.168.1.163:9000/wordcount_checkpoint")

    /**
      *
      * @param key    DStream的key数据类型
      * @param values DStream的value数据类型
      * @param state  是StreamingContext中之前该key的状态值
      * @return
      */
    def mappingFunction(key: String, values: Option[Int], state: State[Long]): (String, Long) = {
      // 获取之前状态的值
      val preStateValue = state.getOption().getOrElse(0L)
      // 计算出当前值
      val currentStateValue = preStateValue + values.getOrElse(0)
      // 更新状态值
      state.update(currentStateValue)
      // 返回结果
      (key, currentStateValue)
    }
    val spec = StateSpec.function[String, Int, Long, (String, Long)](mappingFunction _)

    val kafkaParams = Map(
      "group.id" -> "test-consumer-group",
      "metadata.broker.list" -> "192.168.1.163:9092,192.168.1.164:9092,192.168.1.165:9092",
      "auto.offset.reset" -> "smallest"
    )
    val topics = Set("bigdata")// topics中value是读取数据的线程数量，所以必须大于等于1
    // 创建输入DStream
    //通过 KafkaUtils.createDirectStream接受kafka数据，这里采用是kafka低级api偏移量不受zk管理

    val messages= KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](
      ssc,kafkaParams,topics
    ).map(_._2)


    val resultWordCount: DStream[(String, Long)] = messages
      .filter(line => line.nonEmpty)
      .flatMap(line => line.split(" ").map((_, 1)))
      .reduceByKey(_ + _)
      .mapWithState(spec)

    resultWordCount.print() // 这个也是打印数据

    // 启动开始处理
    ssc.start()
    ssc.awaitTermination() // 等等结束，监控一个线程的中断操作
  }

}
