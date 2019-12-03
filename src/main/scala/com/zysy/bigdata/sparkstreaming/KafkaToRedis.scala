package com.zysy.bigdata.sparkstreaming

import java.util.Date

import kafka.serializer.StringDecoder
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author lj
  * @createDate 2019/12/3 16:00
  **/
object KafkaToRedis {


//  val conf = new SparkConf().setAppName("SparkStreamingKafka")
//  val sc = new SparkContext(conf)
//
//  //过滤商户页PV流量
//  def shopTrafficFilter(log:String):Boolean = {
//    (log contains "\"element_id\":\"pageview\"") &
//      (log contains "\"page_name\":\"shopinfo\"") &
//      ("\"shop_id\":\"[0-9]+\"".r findFirstIn log).nonEmpty
//  }
//
//  //正则表达式提取shopid
//  def shopInfoExtract(log:String) = {
//    val parttern = "\"shop_id\":\"([0-9]+)\"".r
//    val matchResult = parttern findFirstMatchIn log
//    Tuple2(matchResult.get.group(1),1)
//  }
//
//  //计算当前时间距离次日凌晨的时长(毫秒数)
//  def resetTime = {
//    val now = new Date()
//    val tomorrowMidnight = new Date(now.getYear,now.getMonth,now.getDate+1)
//    tomorrowMidnight.getTime - now.getTime
//
//  }
//
//  //商户实时流量状态更新函数
//  val mapFuction = (shopid: String, pv: Option[Int], state: State[Int]) => {
//    val accuSum = pv.getOrElse(0) + state.getOption().getOrElse(0)
//    val output = (shopid,accuSum)
//    state.update(accuSum)
//    output
//  }
//
//  val stateSpec = StateSpec.function(mapFuction)
//
//  while(true){
//
//    val ssc = new StreamingContext(sc, Seconds(30))
//    ssc.checkpoint("./")
//    val kafkaService = new KafkaService
//    val topicName = "log.traffic_data"
//    //从kafka读取日志流
//    val kafkaStream = kafkaService.getKafkaStream[String, StringDecoder](ssc, topicName, Constants.KAFKA_LARGEST_OFFSET)
//    //过滤商户页实时流量
//    val shopTrafficStream = kafkaStream.map(msg => msg._2).filter(shopTrafficFilter).map(shopInfoExtract)
//    //生成商户页流量实时累计状态
//    val shopTrafficUpdateStateDStream = shopTrafficStream.mapWithState(stateSpec).stateSnapshots()
//    //展示商户页实时累计流量TOP10的商户
//    shopTrafficUpdateStateDStream.foreachRDD{
//      rdd => {
//        //取TOP10商户
//        rdd.top(10)(/*自定义排序方法*/TopElementOrdering)
//          .foreach(item => println(item))
//      }
//    }
//
//    ssc.start()
//    //
//    ssc.awaitTerminationOrTimeout(resetTime)
//    ssc.stop(false,true)

 // }

}
