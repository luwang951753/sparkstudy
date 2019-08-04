package spark_1_6

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object Spark_test {

  def main(args: Array[String]): Unit = {

    val sc: SparkContext = new SparkContext()
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.read.json("hdfs://spark1:9000/students.json")
    df.select("name").show()
    df.select(df("name"), df("age") + 1).show()
    df.filter(df("age") > 21).show()
    df.groupBy("age").count().show()



  }

}
