package com.zysy.bigdata.sql

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object Sparksql_typed {

  /***
    * spark sql  typed 操作
    * @param args
    *
    */


  case class Employee(name: String, age: Long, depId: Long, gender: String, salary: Long)


  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local")
      .config("spark.driver.host", "localhost")
      .config("spark.sql.warehouse.dir", "C://Users//lj//Desktop//spark-warehouse").
      //enableHiveSupport(). //启动hive支持
      master("local[4]").getOrCreate()
      // 创建一个 SparkSession 程序入口
    //引入隐式转化  $符号要用
    import spark.implicits._
    //引入聚合函数   avg要用
    import org.apache.spark.sql.functions._

    val department: DataFrame = spark.read.json("D:\\idea\\project\\sparkstudy\\datas\\department.json")
    val employee: DataFrame = spark.read.json("D:\\idea\\project\\sparkstudy\\datas\\employee.json")


   val employeeDS: Dataset[Employee] = employee.as[Employee]

    println(employeeDS.rdd.partitions.length)

    // coalesce和repartition操作
    // 都是用来重新定义分区的
    // 区别在于：coalesce，只能用于减少分区数量，而且可以选择不发生shuffle
    // repartiton，可以增加分区，也可以减少分区，必须会发生shuffle，相当于是进行了一次重分区操作

   val employeeDSRepartitioned: Dataset[Employee] = employeeDS.repartition(7)

    println("==========================================")

    // 看一下它的分区情况
    println(employeeDSRepartitioned.rdd.partitions.length)

    val employeeDSCoalesced: Dataset[Employee] = employeeDSRepartitioned.coalesce(3)

    println(employeeDSCoalesced.rdd.partitions.length)

    employeeDSCoalesced.show




  }

}
