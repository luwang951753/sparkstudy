package com.zysy.bigdata.sparkstreaming.study_and_exercise

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Exercise {
  /***
    * Spark Streaming 的算子操作，包括窗口函数的使用；需要重点掌握
    * 见： https://blog.csdn.net/dabokele/article/details/52602412
    * @param args
    */

  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.ERROR)
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("Exercise")
      .set("spark.driver.host", "localhost")

    val ssc = new StreamingContext(conf, Seconds(1))
    ssc.checkpoint("d://checkpoint")

    val lines = ssc.socketTextStream("192.168.40.130", 9999)
    //1、workcount
    //lines.flatMap(_.split(" ")).map((_,1)).print()
    val words = lines.flatMap(_.split(" "))
    //2、union
//    val wordsOne = words.map(_ + "_one" )
//    val wordsTwo = words.map(_ + "_two" )
//    val unionWords = wordsOne.union(wordsTwo)
//
//    wordsOne.print()
//    wordsTwo.print()
//    print("=================")
//    unionWords.print()

    //6、reduce(func)
//    val reduceWords = words.reduce(_ + "-" + _)
//    reduceWords.print()


    //7、countByValue()
//    val countByValueWords = words.countByValue()
//    countByValueWords.print()

//    /****
//      * 11、transform(func)
//
//　　在Spark-Streaming官方文档中提到，DStream的transform操作极大的丰富了DStream上能够进行的操作内容。
//    使用transform操作后，除了可以使用DStream提供的一些转换方法之外，还能够直接调用任意的调用RDD上的操作函数。
//　　比如下面的代码中，使用transform完成将一行语句分割成单词的功能。
//      */
//    val words = lines.transform(rdd =>
//      rdd.flatMap(_.split(" "))
//    )
//    words.print()

    //TODO 窗口函数的使用
    //1、window(windowLength, slideInterval)
//    val windowWords = lines.window(Seconds( 3 ), Seconds( 1))
//    windowWords.print()

    //2、 countByWindow(windowLength,slideInterval)
//    val windowWords = lines.countByWindow(Seconds( 3 ), Seconds( 1))
//    windowWords.print()

    //3、 reduceByWindow(func, windowLength,slideInterval)
//    val windowWords = lines.reduceByWindow(_ + "-" + _, Seconds( 3) , Seconds( 1 ))
//    windowWords.print()

    //4、reduceByKeyAndWindow(func,windowLength, slideInterval, [numTasks])
    //val windowWords = words.map((_,1)).reduceByKeyAndWindow((a:Int , b:Int) => (a + b) , Seconds(3 ) , Seconds( 1 ))

    //5、reduceByKeyAndWindow(func, invFunc,windowLength, slideInterval, [numTasks])
//    val windowWords = words.map((_,1)).reduceByKeyAndWindow((a: Int, b:Int ) => (a + b) ,
//      (a:Int, b: Int) => (a - b) , Seconds( 3 ), Seconds( 1 ))
//    windowWords.print()

    //6、 countByValueAndWindow(windowLength,slideInterval, [numTasks])
    val windowWords = words.countByValueAndWindow(Seconds( 3 ), Seconds( 1))
    windowWords.print()

    ssc.start()
    ssc.awaitTermination()


  }

}
