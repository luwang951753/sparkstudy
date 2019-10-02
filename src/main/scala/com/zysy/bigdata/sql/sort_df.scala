package com.zysy.bigdata.sql

import org.apache.spark.sql.SparkSession

/***
  * df进行算子排序
  */
object sort_df {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local")
      .config("spark.driver.host", "localhost")
      .config("spark.sql.warehouse.dir", "C://Users//lj//Desktop//spark-warehouse").
      //enableHiveSupport(). //启动hive支持
      master("local[4]").getOrCreate()

    import spark.implicits._

    val data = Array((7, 2, 3), (1, 8, 6), (1, 8, 3), (4, 5, 9))
    val df = spark.createDataFrame(data).toDF("col1", "col2", "col3")
    println("===原始数据===")
    df.show()
    println("===按照col1,col2进行默认排序===")
    // 默认的升序，会按照列的先后顺序进行升序排序
    df.orderBy("col2", "col3").show()

    println("===按照-df(col1)进行升序排序===")
    /**
      * * 此排序方式需要提前创建好df，不能在创建df时使用
     */
    df.orderBy(-df("col2")).show   //取反正序，取正倒序
    df.orderBy($"col2").show

    //===============================================
    println("===按照df(col1).asc,df(col2).desc进行二次排序===")
     /**
       * * 二次排序
       * -号和desc/asc不能在一块使用
        */
    df.orderBy($"col1".asc,$"col2".desc).show
    println("===asc/desc排序方法===")

  }

}
