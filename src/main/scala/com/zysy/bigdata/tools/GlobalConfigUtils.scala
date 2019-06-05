package com.zysy.bigdata.tools

import com.typesafe.config.ConfigFactory

/**
  * @author lj
  * @createDate 2019/6/5 20:57
  **/
class GlobalConfigUtils {
  def conf = ConfigFactory.load()
  def master = conf.getString("master")
  //开始加载spark相关的配置参数
  def sparkWorkTimeout = conf.getString("spark.worker.timeout")
  def sparkRpcTimeout = conf.getString("spark.rpc.askTimeout")
  def sparkNetWorkTimeout = conf.getString("spark.network.timeoout")
  def sparkMaxCores = conf.getString("spark.cores.max")
  def sparkTaskMaxFailures = conf.getString("spark.task.maxFailures")
  def sparkSpeculation = conf.getString("spark.speculation")
  def sparkAllowMutilpleContext = conf.getString("spark.driver.allowMutilpleContext")
  def sparkSerializer = conf.getString("spark.serializer")
  def sparkBuuferSize = conf.getString("spark.buffer.pageSize")

  //cassandra
  def cassandra = conf.getString("spark.cassandra.connection.host")
  def inKeyspace = conf.getString("in.keyspace")
  def outKeyspace = conf.getString("out.keyspace")

  //标签衰减系数
  def coeff = conf.getString("coeff")

}

object GlobalConfigUtils extends GlobalConfigUtils

