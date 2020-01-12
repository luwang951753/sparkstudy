package com.zysy.bigdata.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.junit.Test

class WindowFunction {

  @Test
  def firstSecond(): Unit = {
    val spark = SparkSession.builder()
      .appName("window")
      .master("local[6]").config("spark.driver.host", "localhost")
      .getOrCreate()

    import spark.implicits._

    val data = Seq(
      ("Thin", "Cell phone", 6000),
      ("Normal", "Tablet", 1500),
      ("Mini", "Tablet", 5500),
      ("Ultra thin", "Cell phone", 5000),
      ("Very thin", "Cell phone", 6000),
      ("Big", "Tablet", 2500),
      ("Bendable", "Cell phone", 3000),
      ("Foldable", "Cell phone", 3000),
      ("Pro", "Tablet", 4500),
      ("Pro2", "Tablet", 6500)
    )

    val source = data.toDF("product", "category", "revenue")
//
//    val window: WindowSpec = Window.partitionBy('category)
//      .orderBy('revenue.desc)
//
//    import org.apache.spark.sql.functions._
//
//    source.select('product, 'category, 'revenue, dense_rank() over window as "rank")
//      .where('rank <= 2)
//      .show()

    source.createOrReplaceTempView("productRevenue")

    spark.sql(
      """
        |SELECT
        |product,
        |category,
        |revenue
        |FROM (
        |SELECT
        |product,
        |category,
        |revenue,
        |dense_rank() OVER (PARTITION BY category ORDER BY revenue DESC) as rank
        |FROM productRevenue) tmp
        |WHERE
        |rank <= 2
      """.stripMargin).show()


  }




}
