package com.zysy.bigdata.sql

import org.apache.spark.sql.{DataFrame, SparkSession}

object Sparksql_untyped {

  /***
    * spark sql  untyped 操作
    * @param args
    *
    *             /**
    * 计算部门的平均薪资和年龄
    *
    * 需求：
    * 		1、只统计年龄在20岁以上的员工
    * 		2、根据部门名称和员工性别为粒度来进行统计
    * 		3、统计出每个部门分性别的平均薪资和年龄
    *
    */
    */
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


    employee.filter("age > 20").join(department,$"depId" === $"id").
      groupBy(department("name"),employee("gender")).
      agg(avg(employee("salary")),avg(employee("age"))).show(

    )




  }

}
