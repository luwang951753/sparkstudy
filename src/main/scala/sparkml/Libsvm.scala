package sparkml

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Libsvm {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("testLabeledPoint2") //建立本地环境变量
    val sc = new SparkContext(conf) //建立Spark处理
    val mu: RDD[LabeledPoint] = MLUtils.loadLibSVMFile(sc, "D://a.txt") //读取文件
    mu.foreach(println) //打印内容(1.0,(3,[0,1,2],[2.0,3.0,5.0]))
  }

}
