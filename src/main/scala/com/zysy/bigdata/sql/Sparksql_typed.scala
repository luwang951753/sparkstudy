package com.zysy.bigdata.sql

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object Sparksql_typed {

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
    //引入聚合函数   avg要用
    import org.apache.spark.sql.functions._

    spark.sparkContext.setLogLevel("WARN")

    val department: DataFrame = spark.read.json("D:\\idea\\project\\sparkstudy\\datas\\department.json")
    val employee: DataFrame = spark.read.json("D:\\idea\\project\\sparkstudy\\datas\\employee.json")


   val employeeDS: Dataset[Employee] = employee.as[Employee]
   val departmentDS: Dataset[Department] = department.as[Department]

    println(employeeDS.rdd.partitions.length)

    // coalesce和repartition操作
    // 都是用来重新定义分区的
    // 区别在于：coalesce，只能用于减少分区数量，而且可以选择不发生shuffle
    // repartiton，可以增加分区，也可以减少分区，必须会发生shuffle，相当于是进行了一次重分区操作

//   val employeeDSRepartitioned: Dataset[Employee] = employeeDS.repartition(7)
//
//    println("==========================================")
//
//    // 看一下它的分区情况
//    println(employeeDSRepartitioned.rdd.partitions.length)
//
//    val employeeDSCoalesced: Dataset[Employee] = employeeDSRepartitioned.coalesce(3)
//
//    println(employeeDSCoalesced.rdd.partitions.length)
//
//    employeeDSCoalesced.show



    // distinct和dropDuplicates
    // 都是用来进行去重的，区别在哪儿呢？
    // distinct，是根据每一条数据，进行完整内容的比对和去重
    // dropDuplicates，可以根据指定的字段进行去重

    //    val distinctEmployeeDS = employeeDS.distinct();
    //    distinctEmployeeDS.show()
    //    val dropDuplicatesEmployeeDS = employeeDS.dropDuplicates(Seq("name"))
    //    dropDuplicatesEmployeeDS.show()

    // except：获取在当前dataset中有，但是在另外一个dataset中没有的元素
    // filter：根据我们自己的逻辑，如果返回true，那么就保留该元素，否则就过滤掉该元素
    // intersect：获取两个数据集的交集

    //    employeeDS.except(employeeDS2).show()
    //    employeeDS.filter { employee => employee.age > 30 }.show()
    //    employeeDS.intersect(employeeDS2).show()

    // map：将数据集中的每条数据都做一个映射，返回一条新数据
    // flatMap：数据集中的每条数据都可以返回多条数据
    // mapPartitions：一次性对一个partition中的数据进行处理

    employeeDS.map {employee => (employee.name, employee.salary + 1000) }.show()
    departmentDS.flatMap {
      department => Seq(Department(department.id + 1, department.name + "_1"), Department(department.id + 2, department.name + "_2"))
    }.show()
    employeeDS.mapPartitions { employees => {
      val result = scala.collection.mutable.ArrayBuffer[(String, Long)]()
      while(employees.hasNext) {
        var emp = employees.next()
        result += ((emp.name, emp.salary + 1000))
      }
      result.iterator
    }
    }.show()
  }



}
