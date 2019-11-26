package sparkml

import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg
import org.apache.spark.ml.linalg.Vectors


object Test {
  def main(args: Array[String]): Unit = {

    /*val vd: linalg.Vector = Vectors.dense(2, 0, 6) //建立密集向量
    println(vd(2)) //打印稀疏向量第3个值
    val vs: linalg.Vector = Vectors.sparse(4, Array(0, 1, 2, 3), Array(9, 5, 2, 7)) //建立稀疏向量
    //第一个参数4代表输入数据的大小，一般要求大于等于输入的数据值，第二个参数是数据下标，第三个参数是数据值
    println(vs) //打印稀疏向量第3个值

    //通过指定其非零条目来创建稀疏向量（1.0,0.0,3.0）
    val sv2: linalg.Vector = Vectors.sparse(3, Seq((0, 1.0), (2, 3.0)))
    println(sv2(0))*/

    val vd1: linalg.Vector =  Vectors.dense(2, 0, 6)                 //建立密集向量
    val pos = LabeledPoint(1, vd1)                         //对密集向量建立标记点
    println(pos.features)                               //打印标记点内容数据
    println(pos.label)                                  //打印既定标记
    val vs1: linalg.Vector = Vectors.sparse(4, Array(0,1,2,3), Array(9,5,2,7))      //建立稀疏向量
    val neg = LabeledPoint(2, vs1)                           //对密集向量建立标记点
    println(neg.features)                                //打印标记点内容数据
    println(neg.label)




  }

}
