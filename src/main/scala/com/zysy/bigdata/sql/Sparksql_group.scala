package com.zysy.bigdata.sql

import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, SQLContext, SparkSession}

/**
  * @author lj
  * @createDate 2019/9/3 17:35
  *
  * 见：https://blog.csdn.net/u011622631/article/details/84786777
  **/


object Sparksql_group {
  case class MemberOrderInfo(area:String,memberType:String,product:String,price:Int)

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local")
      .config("spark.driver.host", "localhost")
      .config("spark.sql.warehouse.dir", "C://Users//lj//Desktop//spark-warehouse").
      //enableHiveSupport(). //启动hive支持
      master("local[4]").getOrCreate()

    val sqlContext:SQLContext = spark.sqlContext
    // 创建一个 SparkSession 程序入口
    //引入隐式转化  $符号要用
    import spark.implicits._
    //引入聚合函数   avg要用
    //import org.apache.spark.sql.functions._
    //import sqlContext.implicits._  TODO 这个会和  import spark.implicits._ 进行冲突


    spark.sparkContext.setLogLevel("WARN")

    //val personEncoder: Encoder[MemberOrderInfo] = Encoders.product


    val orders=Seq(
      MemberOrderInfo("深圳","钻石会员","钻石会员1个月",25),
      /*MemberOrderInfo("深圳","钻石会员","钻石会员1个月",25),
      MemberOrderInfo("深圳","钻石会员","钻石会员3个月",70),
      MemberOrderInfo("深圳","钻石会员","钻石会员12个月",300),
      MemberOrderInfo("深圳","铂金会员","铂金会员3个月",60),
      MemberOrderInfo("深圳","铂金会员","铂金会员3个月",60),
      MemberOrderInfo("深圳","铂金会员","铂金会员6个月",120),
      MemberOrderInfo("深圳","黄金会员","黄金会员1个月",15),
      MemberOrderInfo("深圳","黄金会员","黄金会员1个月",15),
      MemberOrderInfo("深圳","黄金会员","黄金会员3个月",45),
      MemberOrderInfo("深圳","黄金会员","黄金会员12个月",180),*/
      MemberOrderInfo("北京","钻石会员","钻石会员1个月",25),
      MemberOrderInfo("北京","钻石会员","钻石会员1个月",25),
      /*MemberOrderInfo("北京","铂金会员","铂金会员3个月",60),
      MemberOrderInfo("北京","黄金会员","黄金会员3个月",45),*/
      /*MemberOrderInfo("上海","钻石会员","钻石会员1个月",25),
      MemberOrderInfo("上海","钻石会员","钻石会员1个月",25),*/
      MemberOrderInfo("上海","铂金会员","铂金会员3个月",60),
      MemberOrderInfo("上海","黄金会员","黄金会员3个月",45)
    )
    //把seq转换成DataFrame
    val ds:Dataset[MemberOrderInfo] = orders.toDS()
    //把DataFrame注册成临时表
    ds.createOrReplaceTempView("orderTempTable")


    //TODO 1.group by
//    val df= sqlContext.sql("select area,memberType,product,sum(price) as total " +
//      "from orderTempTable group by area,memberType,product")
    //df.show()

    //TODO 2.grouping sets
    //
    //a.grouping sets是group by子句更进一步的扩展,
    // 它让你能够定义多个数据分组。这样做使聚合更容易, 并且因此使得多维数据分析更容易。
    //b.够用grouping sets在同一查询中定义多个分组


   /* val df = sqlContext.sql("select area,memberType,product,sum(price) as total " +
      "from orderTempTable group by area,memberType,product grouping sets(area,memberType,product)")
    df.show()*/

    /***
      * 上面的语句输出结果如下，可以看到使用grouping sets(area,memberType,product)会分别对这3个维度进行group by，
      * 也可以grouping sets ((area,memberType),(area,product)))此时相当于group by (area,memberType) union group by
      * (area,product),也就是说grouping sets 后面可以指定你想要的各种维度组合。
      * ————————————————
      * 版权声明：本文为CSDN博主「腾飞的大象」的原创文章，遵循 CC 4.0 BY-SA 版权协议，转载请附上原文出处链接及本声明。
      * 原文链接：https://blog.csdn.net/u011622631/article/details/84786777
      */


//    //TODO 3.grouping by area,memberType,product with rollup
//    val df = sqlContext.sql("select area,memberType,product,sum(price) as total " +
//      "from orderTempTable group by area,memberType,product with rollup")
//
//    df.show()



    //TODO 4.grouping by area,memberType,product with rollup

    val  df= sqlContext.sql("select area,memberType,product,sum(price) as total " +
  "from orderTempTable group by area,memberType,product with cube")

    df.show()











  }

}
