package spark_1_6

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel

object Spark_test {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("DailySale")
      .set("spark.driver.host", "localhost")
    val sc: SparkContext = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
//    val df = sqlContext.read.json("hdfs://spark1:9000/students.json")
//    df.select("name").show()
//    df.select(df("name"), df("age") + 1).show()
//    df.filter(df("age") > 21).show()
//    df.groupBy("age").count().show()
    val data = Seq("A","A","B","C")
    val mapData = Map("A"->2,"A"->1,"B"->1,"C"->2,"A"->10)

    val value: RDD[String] = sc.parallelize(data)
    val unit: RDD[(String, Int)] = value.map(x=>(x,1))
    unit.distinct()
    val re: RDD[(String, Int)] = unit.reduceByKey((x,y)=>x)
    re.persist(StorageLevel.MEMORY_AND_DISK_SER_2)

    re.foreach(print)


  }

}
