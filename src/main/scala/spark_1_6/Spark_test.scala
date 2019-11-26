package spark_1_6

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

object Spark_test {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("DailyUV")
      .set("spark.driver.host", "localhost")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
//    val df = sqlContext.read.json("hdfs://spark1:9000/students.json")
//    df.select("name").show()
//    df.select(df("name"), df("age") + 1).show()
//    df.filter(df("age") > 21).show()
//    df.groupBy("age").count().show()

    val words = Array("one", "two", "two", "three", "three", "three")
    val wordPairsRDD = sc.parallelize(words).map(word => (word, 1))

    val unit: RDD[(String, Iterable[Int])] = wordPairsRDD.groupByKey()
    unit.foreach(print)
//    unit.flatMap{
//      case (mid, startulogItr) =>
//        startulogItr.take(1)
//
//    }.foreach(println)






  }

}
