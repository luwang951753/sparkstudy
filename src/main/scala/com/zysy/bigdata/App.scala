package com.zysy.bigdata

import com.zysy.bigdata.tools.GlobalConfigUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}


/**
  * @author lj
  * @createDate 2019/6/5 20:57
  **/
object App {
  def main(args: Array[String]): Unit = {

    @transient
    val sparkConf = new SparkConf().setAppName("APP")
      .setMaster(GlobalConfigUtils.master)
      .set("spark.worker.timeout", GlobalConfigUtils.sparkWorkTimeout)
      .set("spark.cores.max", GlobalConfigUtils.sparkMaxCores)
      .set("spark.rpc.askTimeout", GlobalConfigUtils.sparkRpcTimeout)
      .set("spark.task.macFailures", GlobalConfigUtils.sparkTaskMaxFailures)
      .set("spark.speculation", GlobalConfigUtils.sparkSpeculation)
      .set("spark.driver.allowMutilpleContext", GlobalConfigUtils.sparkAllowMutilpleContext)
      .set("spark.serializer", GlobalConfigUtils.sparkSerializer)
      .set("spark.buffer.pageSize", GlobalConfigUtils.sparkBuuferSize)
      .set("spark.cassandra.connection.host", GlobalConfigUtils.cassandra)

    val sparkContext = SparkContext.getOrCreate(sparkConf)


    val sparksession = SparkSession.builder().config(sparkConf).getOrCreate()
    val sqlContext = sparksession.sqlContext


    if (!sparkContext.isStopped) sparkContext.stop()
  }
}



