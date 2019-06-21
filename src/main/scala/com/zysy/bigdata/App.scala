package com.zysy.bigdata

import com.datastax.spark.connector.cql.CassandraConnector
import com.zysy.bigdata.tools.GlobalConfigUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}


/**
  * @author lj
  * @createDate 2019/6/5 20:57
  **/
object App extends Logging{
  Logger.getRootLogger.setLevel(Level.WARN)

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
    //sparkContext.setLogLevel("WARN")
    val sparksession = SparkSession.builder().config(sparkConf).getOrCreate()
    val sqlContext = sparksession.sqlContext


    //cassandra的链接
    val connector = CassandraConnector.apply(sparkConf)
    val session = connector.openSession


    //具体逻辑
    println("===============================")
    //logInfo("===============================")




    if (!sparkContext.isStopped) sparkContext.stop()
  }
}



