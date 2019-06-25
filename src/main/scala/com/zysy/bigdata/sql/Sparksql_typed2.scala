package com.zysy.bigdata.sql

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object Sparksql_typed2 {

  /***
    * spark sql  typed 操作
    * @param args
    *
    */


  case class Employee(name: String, age: Long, depId: Long, gender: String, salary: Long)

  case class Department(id: Long, name: String)


  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local")
      .config("spark.driver.host", "localhost")
      .config("spark.sql.warehouse.dir", "C://Users//lj//Desktop//spark-warehouse").
      //enableHiveSupport(). //启动hive支持
      master("local[4]").getOrCreate()
      // 创建一个 SparkSession 程序入口
    //引入隐式转化  $符号要用
    import spark.implicits._
    import org.apache.spark.sql.functions._
    //引入聚合函数   avg要用

    spark.sparkContext.setLogLevel("WARN")

    val department: DataFrame = spark.read.json("D:\\idea\\project\\sparkstudy\\datas\\department.json")
    val employee: DataFrame = spark.read.json("D:\\idea\\project\\sparkstudy\\datas\\employee.json")


   val employeeDS: Dataset[Employee] = employee.as[Employee]
   val departmentDS: Dataset[Department] = department.as[Department]

    // collect_list，就是将一个分组内，指定字段的值都收集到一起，不去重
    // collect_set，同上，但是唯一的区别是，会去重

    //    employee
    //        .join(department, $"depId" === $"id")
    //        .groupBy(department("name"))
    //        .agg(avg(employee("salary")), sum(employee("salary")), max(employee("salary")), min(employee("salary")), count(employee("name")), countDistinct(employee("name")))
    //        .show()

    // collect_list和collect_set，都用于将同一个分组内的指定字段的值串起来，变成一个数组
    // 常用于行转列
    // 比如说
    // depId=1, employee=leo
    // depId=1, employee=jack
    // depId=1, employees=[leo, jack]

    employee.show()
//    +---+-----+------+------+------+
//    |age|depId|gender|  name|salary|
//    +---+-----+------+------+------+
//    | 25|    1|  male|   Leo| 20000|
//      | 30|    2|female| Marry| 25000|
//      | 35|    1|  male|  Jack| 15000|
//      | 42|    3|  male|   Tom| 18000|
//      | 21|    3|female|Kattie| 21000|
//      | 30|    2|female|   Jen| 28000|
//      | 19|    2|female|   Jen|  8000|
//      +---+-----+------+------+------+

    employee
      .groupBy(employee("depId"))
      .agg(collect_set(employee("name")), collect_list(employee("name")))
      .collect()
      .foreach(println(_))

//      [1,WrappedArray(Jack, Leo),WrappedArray(Leo, Jack)]
//    [3,WrappedArray(Tom, Kattie),WrappedArray(Tom, Kattie)]
//    [2,WrappedArray(Marry, Jen),WrappedArray(Marry, Jen, Jen)]

    // 日期函数：current_date、current_timestamp
    // 数学函数：round
    // 随机函数：rand
    // 字符串函数：concat、concat_ws
    // 自定义udf和udaf函数

    // http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions$

    employee
      .select(employee("name"), current_date(), current_timestamp(), rand(), round(employee("salary"), 2), concat(employee("gender"), employee("age")), concat_ws("|", employee("gender"), employee("age")))
      .show()







  }

}
