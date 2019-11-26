import java.util

import org.apache.log4j.Logger
import org.apache.spark.internal.Logging
import org.apache.spark.{SparkConf, SparkContext}
import sun.util.logging.PlatformLogger
import sun.util.logging.PlatformLogger.Level

import scala.collection.mutable

/**
  * @author lj
  * @createDate 2019/6/11 10:49
  **/
object SparkTest extends Logging {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("test")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    //Logger.getRootLogger.setLevel(Level.INFO)
    /*val rdd = List(1,2,3,4,5,6,7,8,9)
    rdd.par.aggregate()*/

    //val pairRDD = sc.parallelize(List( ("cat",2), ("cat", 5), ("mouse", 4),("cat", 12), ("dog", 12), ("mouse", 2)), 2)
    val pairRDD = sc.parallelize(List( ("cat","2"), ("cat", "5"), ("mouse", "4"),("cat", "12"), ("dog", "12"), ("mouse", "2")), 2)
    /* def func2(index: Int, iter: Iterator[(String, Int)]) : Iterator[String] = {
      iter.toList.map(x => "[partID:" +  index + ", val: " + x + "]").iterator
    }
    val result = pairRDD.mapPartitionsWithIndex(func2).collect
    result.foreach(print)
    pairRDD.aggregateByKey(0)(math.max(_, _), _ + _).collect.foreach(println)*/
    pairRDD.aggregateByKey("xxx")(_ + _ , _ + _).collect.foreach(println)

    /*val rdd1 = sc.parallelize(List(1,2,3,4,5,6,7,8,9), 2)
    val aa = rdd1.aggregate(5)(math.max(_, _), _ + _)
    println(aa)*/

    //val x  = sc.parallelize(1 to 10, 3)
    //x.glom()
    //x.flatMap(List.fill(scala.util.Random.nextInt(10))(_)).collect.foreach(println)
    //x.flatMap(List.fill(2)(_)).collect.foreach(print)


    val a = sc.parallelize(List("dog","cat","gnu","salmon","rabbit","turkey","wolf","bear","bee"), 3)
    val b = sc.parallelize(List(1,1,2,2,2,1,2,2,2), 3)
    val c = b.zip(a)
    val d = c.combineByKey(List(_), (x:List[String], y:String) => y :: x, (x:List[String], y:List[String]) => x ::: y)
    /*d.collect.foreach(print)*/
    //res16: Array[(Int, List[String])] = Array((1,List(cat, dog, turkey)), (2,List(gnu, rabbit, salmon, bee, bear, wolf)))

    logInfo("ashflasjfa ")

    val topicThreadMap: mutable.HashMap[String, Int] = new mutable.HashMap[String, Int]
    topicThreadMap += ("wordcount" -> 1)
    topicThreadMap += ("wordcount2" -> 5)
    topicThreadMap += ("wordcount3" -> 6)

    println(Map(topicThreadMap.mapValues(_.intValue()).toSeq: _*))

    val re = Map(topicThreadMap.mapValues(_.intValue()).toSeq: _*)
    println(re)




  }

}
