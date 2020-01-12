package com.zysy.bigdata.sql

import org.apache.spark.sql.SparkSession
import org.junit.Test

class JoinProcessor {
  val spark = SparkSession.builder()
    .master("local[6]").config("spark.driver.host", "localhost")
    .appName("join")
    .getOrCreate()

  import spark.implicits._

  private val person = Seq((0, "Lucy", 0), (1, "Lily", 0), (2, "Tim", 2), (3, "Danial", 3))
    .toDF("id", "name", "cityId")
  person.createOrReplaceTempView("person")

  private val cities = Seq((0, "Beijing"), (1, "Shanghai"), (2, "Guangzhou"))
    .toDF("id", "name")
  cities.createOrReplaceTempView("cities")

  @Test
  def introJoin(): Unit = {
    val person = Seq((0, "Lucy", 0), (1, "Lily", 0), (2, "Tim", 2), (3, "Danial", 0))
      .toDF("id", "name", "cityId")

    val cities = Seq((0, "Beijing"), (1, "Shanghai"), (2, "Guangzhou"))
      .toDF("id", "name")

    val df = person.join(cities, person.col("cityId") === cities.col("id"))
      .select(person.col("id"),
        person.col("name"),
        cities.col("name") as "city")
//      .show()
    df.createOrReplaceTempView("user_city")

    spark.sql("select id, name, city from user_city where city = 'Beijing'")
      .show()
  }

  @Test
  def crossJoin(): Unit = {
    person.crossJoin(cities)
      .where(person.col("cityId") === cities.col("id"))
      .show()

    spark.sql("select u.id, u.name, c.name from person u cross join cities c " +
      "where u.cityId = c.id")
      .show()
  }

  @Test
  def inner(): Unit = {
    person.join(cities,
      person.col("cityId") === cities.col("id"),
      joinType = "inner")
      .show()

    spark.sql("select p.id, p.name, c.name " +
      "from person p inner join cities c on p.cityId = c.id")
      .show()
  }

  @Test
  def fullOuter(): Unit = {
    // 内连接, 就是只显示能连接上的数据, 外连接包含一部分没有连接上的数据, 全外连接, 指左右两边没有连接上的数据, 都显示出来
    person.join(cities,
      person.col("cityId") === cities.col("id"),
      joinType = "full")
      .show()

    spark.sql("select p.id, p.name, c.name " +
      "from person p full outer join cities c " +
      "on p.cityId = c.id")
      .show()
  }

  @Test
  def leftRight(): Unit = {
    // 左连接
    person.join(cities,
      person.col("cityId") === cities.col("id"),
      joinType = "left")
      .show()

    spark.sql("select p.id, p.name, c.name " +
      "from person p left join cities c " +
      "on p.cityId = c.id")
      .show()

    // 右连接
    person.join(cities,
      person.col("cityId") === cities.col("id"),
      joinType = "right")
      .show()

    spark.sql("select p.id, p.name, c.name " +
      "from person p right join cities c " +
      "on p.cityId = c.id")
      .show()
  }

  @Test
  def leftAntiSemi(): Unit = {
    // 左连接 anti
    person.join(cities,
      person.col("cityId") === cities.col("id"),
      joinType = "leftanti")
      .show()

    spark.sql("select p.id, p.name " +
      "from person p left anti join cities c " +
      "on p.cityId = c.id")
      .show()

    // 右连接
    person.join(cities,
      person.col("cityId") === cities.col("id"),
      joinType = "leftsemi")
      .show()

    spark.sql("select p.id, p.name " +
      "from person p left semi join cities c " +
      "on p.cityId = c.id")
      .show()
  }


  @Test
  def mapJoin(): Unit = {
//    val personRDD = spark.sparkContext.parallelize(Seq((0, "Lucy", 0),
//      (1, "Lily", 0), (2, "Tim", 2), (3, "Danial", 3)))
//
//    val citiesRDD = spark.sparkContext.parallelize(Seq((0, "Beijing"),
//      (1, "Shanghai"), (2, "Guangzhou")))
//
//    val citiesBroadcast = spark.sparkContext.broadcast(citiesRDD.collectAsMap())
//
//    val result = personRDD.mapPartitions(
//      iter => {
//        val citiesMap = citiesBroadcast.value
//        // 使用列表生成式 yield 生成列表
//        val result = for (person <- iter if citiesMap.contains(person._3))
//          yield (person._1, person._2, citiesMap(person._3))
//        result
//      }
//    ).collect()
//
//    result.foreach(println(_))

    println(spark.conf.get("spark.sql.autoBroadcastJoinThreshold").toInt / 1024 / 1024)

    println(person.crossJoin(cities).queryExecution.sparkPlan.numberedTreeString)
    //当关闭这个参数的时候, 则不会自动 Map 端 Join 了
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    println(person.crossJoin(cities).queryExecution.sparkPlan.numberedTreeString)

    import org.apache.spark.sql.functions._
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    println(person.crossJoin(broadcast(cities)).queryExecution.sparkPlan.numberedTreeString)


    //即使是使用 SQL 也可以使用特殊的语法开启
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    val resultDF = spark.sql(
      """
        |select /*+ MAPJOIN (rt) */ * from person cross join cities rt
      """.stripMargin)
    println(resultDF.queryExecution.sparkPlan.numberedTreeString)
  }



}
