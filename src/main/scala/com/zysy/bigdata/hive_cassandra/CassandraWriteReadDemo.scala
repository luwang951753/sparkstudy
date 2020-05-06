package com.zysy.bigdata.hive_cassandra

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * @author lj
  * @createDate 2020/5/6 16:17
  *            https://blog.csdn.net/weixin_42003671/article/details/86578798
  **/
class CassandraWriteReadDemo {

  object CassandraWriteReadDemo {
    def main(args: Array[String]): Unit = {
      val spark = SparkSession.builder
        .appName("capCassandraUpdater")
        .master("local[*]")
        // spark.cassandra.connection.host 可以写入Cassandra多个节点，形成高可用
        .config("spark.cassandra.connection.host", "10.11.12.14,10.11.12.20,10.11.12.23")
        .config("spark.sql.shuffle.partitions",20)
        .enableHiveSupport()
        .getOrCreate()
      //读取hive中的表
      val dfFromHive = spark.sql("select * from .aaronteststu_info")
      //将df写入到Cassandra中
      dfFromHive.write
        .format("org.apache.spark.sql.cassandra") //format一定要写
        .option("keyspace", "aarontest") //cassandra的keyspace相当于mysql中的库
        .option("table", "c_staff")
        .option("column", "name")
        .option("column","age")
        //强一致性 ALL，弱一致性ANY 或 ONE 默认是LOCAL_QUORUM，一般为了提高写入效率设置为ONE
        .option("spark.cassandra.output.consistency.level", "ALL")
        .mode(SaveMode.Append)
        .save()
      //从Cassandra读取数据
      val dfFormCassandra = spark.read
        .format("org.apache.spark.sql.cassandra")
        .option("keyspace", "aarontest").option("table", "c_staff").load()
      //显示五行Cassandra数据
      dfFormCassandra.show(5,false)
      spark.stop()
    }
  }

}
