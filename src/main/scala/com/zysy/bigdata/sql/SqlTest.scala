package com.zysy.bigdata.sql

import org.apache.spark.sql.{Row, SparkSession}

object SqlTest {

  case class Person(name: String, age: Long)

  case class Record(key: Int, value: String)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .config("spark.driver.host", "localhost")
        .config("spark.sql.warehouse.dir", "C://Users//lj//Desktop//spark-warehouse").
      enableHiveSupport().//启动hive支持
      master("local[4]").getOrCreate()  // 创建一个 SparkSession 程序入口
    import spark.implicits._
    import spark.sql
    spark.sparkContext.setLogLevel("WARN")

    sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)")
    sql("LOAD DATA LOCAL INPATH 'D:\\\\idea\\\\project\\\\sparkstudy\\\\datas\\\\kv1.txt' INTO TABLE src")
    sql("SELECT * FROM src").show()
    sql("SELECT COUNT(*) FROM src").show()

    val sqlDF = sql("SELECT key, value FROM src WHERE key < 10 ORDER BY key")
    val stringsDS = sqlDF.map {
      case Row(key: Int, value: String) => s"Key: $key, Value: $value"
    }
    stringsDS.show()

    val recordsDF = spark.createDataFrame((1 to 100).map(i => Record(i, s"val_$i")))
    recordsDF.createOrReplaceTempView("records")
    sql("SELECT * FROM records r JOIN src s ON r.key = s.key").show()


    //    val df = spark.read.json("D:\\idea\\project\\sparkstudy\\datas\\people.json")
//    df.show()
//    df.printSchema()
//    df.select("name").show()
//   // df.select($"name").show()
//    df.select($"name", $"age" + 1).show()
////    df.filter($"age" > 21).show()
////    df.groupBy("age").count().show()
//
//
//    df.createOrReplaceTempView("people")
//    val sqlDF = spark.sql("SELECT * FROM people")
//    //sqlDF.show()
//
//
//
//    val caseClassDS = Seq(Person("Andy", 32)).toDS()
//    caseClassDS.show()
//    val primitiveDS = Seq(1, 2, 3).toDS()
//    primitiveDS.map(_ + 1).collect()
//    val path = "D:\\idea\\project\\sparkstudy\\datas\\people.json"
//    val peopleDS = spark.read.json(path).as[Person]
//    peopleDS.show()


  }



}



