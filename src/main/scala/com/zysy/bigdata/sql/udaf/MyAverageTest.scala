package com.zysy.bigdata.sql.udaf

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._


/**
  * 用户自定义集成算子Demo
  */
object MyAverageTest {


  /**
    * 读取itemdata.data数据，计算平均score
    */
  class MyAverage extends UserDefinedAggregateFunction{
    /**
      * 计算平均score，输入的应该是score这一列数据
      * StructField定义了列字段的名称score_column,字段的类型Double
      * StructType要求输入数StructField构成的数组Array，这里只有一列，所以与Nil运算生成Array
      * @return StructType
      */
    override def inputSchema: StructType = StructType(
      StructField("score_column",DoubleType)::Nil)

    /**
      * 缓存Schema，存储中间计算结果，
      * 比如计算平均score,需要计算score的总和和score的个数,然后average(score)=sum(score)/count(score)
      * 所以这里定义了StructType类型：两个StructField字段：sum和count
      * @return StructType
      */
    override def bufferSchema: StructType = StructType(
      StructField("sum",DoubleType)::StructField("count",LongType)::Nil)

    /**
      * 自定义集成算子最终返回的数据类型
      * 也就是average(score)的类型，所以是Double
      * @return DataType 返回数据类型
      */
    override def dataType: DataType = DoubleType

    /**
      * 数据一致性检验：对于同样的输入，输出是一样的
      * @return Boolean true 同样的输入，输出也一样
      */
    override def deterministic: Boolean = true

    /**
      * 初始化缓存sum和count
      * sum=0.0，count=0
      * @param buffer 中间数据
      */
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      //sum=0.0
      buffer(0)=0.0
      //count=0
      buffer(1)=0L
    }

    /**
      * 每次计算更新缓存
      * @param buffer 缓存数据
      * @param input 输入数据score
      */
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      //输入非空
      if(!input.isNullAt(0)){
        //sum=sum+输入的score
        buffer(0)=buffer.getDouble(0)+input.getDouble(0)
        //count=count+1
        buffer(1)=buffer.getLong(1)+1
      }
    }

    /**
      * 将更新后的buffer存储到缓存
      * @param buffer1 缓存
      * @param buffer2 更新后的buffer
      */
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1(0)=buffer1.getDouble(0)+buffer2.getDouble(0)
      buffer1(1)=buffer1.getLong(1)+buffer2.getLong(1)
    }

    /**
      * 计算最终的结果：average(score)=sum(score)/count(score)
      * @param buffer
      * @return
      */
    override def evaluate(buffer: Row): Double = buffer.getDouble(0)/buffer.getLong(1)

  }


  def main(args: Array[String]): Unit = {
    //创建Spark SQL切入点
    val spark = SparkSession.builder().master("local").appName("My-Average").getOrCreate()
    //注册名为myAverage的自定义集成算子MyAverage
    spark.udf.register("myAverage",new MyAverage)
    //读取HDFS文件系统数据itemdata.data转换成指定列名的DataFrame
    val dataDF=spark.read.csv("hdfs://192.168.189.21:8020/input/mahout-demo/itemdata.data").toDF("user_id","item_id","score")
    //创建临时视图
    dataDF.createOrReplaceTempView("data")
    //通过sql计算平均工资
    spark.sql("select myAverage(score) as average_score from data").show()
  }

}
