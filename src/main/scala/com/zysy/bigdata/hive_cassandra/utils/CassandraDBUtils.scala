package com.zysy.bigdata.hive_cassandra.utils

import java.io.IOException

import com.datastax.spark.connector.cql.CassandraConnector
import com.zysy.bigdata.tools.GlobalConfigUtils
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.immutable.HashMap
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by lj
  */
object CassandraDBUtils {

  val keyspace:String = GlobalConfigUtils.outKeyspace

  //系统配置
  val systemOptions: Map[String, String] = HashMap(
    "keyspace" -> "system_schema",
    "table" -> "tables"
  )

  val cassandraOptions:mutable.HashMap[String, String] = mutable.HashMap("keyspace" -> "sy3_test")


  def isTableExist(sparkSession: SparkSession, table_name: String): Boolean = {


    val dataset = sparkSession.read.format("org.apache.spark.sql.cassandra")
      .options(systemOptions).load()
    val result = dataset.where(s"keyspace_name = '$keyspace' and table_name = '$table_name'")
    result.collect().length > 0
  }


  /***
    * 从cassandra读取 数据下沉到cassandra中
    * @param sparkSession
    * @param table
    * @return
    */
  def getDataFrameFromTable(sparkSession:SparkSession,table:String):  DataFrame={

    sparkSession.read.format("org.apache.spark.sql.cassandra")
      .options(HashMap(
        "keyspace" -> s"$keyspace",
        "table" -> table.toLowerCase
      )).load()

  }


  def process(sparkSession: SparkSession,
              connector: CassandraConnector,
              data: DataFrame,
              TO_TABLENAME: String
             ): Unit = {
    //TODO 1:如果表不存在，则创建表
    if (!isTableExist(sparkSession, TO_TABLENAME)) {
      createKeyspaceAndTable(connector, TO_TABLENAME, data.schema)
    }

    //TODO 数据下沉到cassandra中
    //cassandraOptions += ("table" -> TO_TABLENAME.toLowerCase())
    data.write.format("org.apache.spark.sql.cassandra")
          .options(HashMap(
            "keyspace" -> s"$keyspace",
            "table" -> TO_TABLENAME.toLowerCase
          )).mode("append").save()
  }


  /**
    * create keyspace and table if not exists
    * @return
    */

  def createKeyspaceAndTable(connector: CassandraConnector, table: String,
                             userSpecifiedSchema: StructType) = {
    try {
      connector.withSessionDo {
        val structType: StructType = userSpecifiedSchema
        val builder = new StringBuilder
        builder.append("CREATE TABLE IF NOT EXISTS ")
        builder.append(s"$keyspace")
        builder.append(".")
        builder.append(s"$table")
        builder.append(" (")
        builder.append(structType.sql.replace("STRUCT<", "").replace(">", "").replace("`", "\"").replace(":", "").replace("STRING", "TEXT"))
        builder.append(",PRIMARY KEY ((")
        //        val fieldsNames = structType.fieldNames
        //        for (i <- 0 until fieldsNames.length) {
        //          builder.append(fieldsNames(i))
        //          if (i < fieldsNames.length - 1) {
        //            builder.append(",")
        //          }
        //        }
        val fields = structType.fields
        val partitionKeyColumns: ArrayBuffer[String] = ArrayBuffer[String]() //partition keys array
        val clusteringColumns: ArrayBuffer[String] = ArrayBuffer[String]() //clustering keys array
        var firstFieldName = fields(0).name //first column's name. Used as default primary key when user not specified partition key or clustering key

        for (i <- fields.indices) {
          val comment = fields(i).getComment()
          //fetch first column name to be used as default primary key

          if (0 == structType.fieldIndex(fields(i).name)) {
            firstFieldName = fields(i).name
          }
          //fetch partition key
          if (comment.getOrElse("").contains("_pk")) {
            partitionKeyColumns += fields(i).name
          }
          else if (comment.getOrElse("").contains("_ck")) {
            clusteringColumns += fields(i).name //fetch clustering key
          }
        }
        if (partitionKeyColumns.size <= 0 && clusteringColumns.size <= 0) {
          builder.append(firstFieldName)
          builder.append(")")
        }
        else if (partitionKeyColumns.size <= 0 && clusteringColumns.nonEmpty) {
          throw new IOException("Please specify partition key")
        }
        else {
          for (i <- partitionKeyColumns.indices) {
            builder.append(partitionKeyColumns(i))
            if (i != partitionKeyColumns.size - 1) {
              builder.append(",")
            }
          }
          builder.append(")")
          if (clusteringColumns.nonEmpty) {
            builder.append(",")
            for (i <- clusteringColumns.indices) {
              builder.append(clusteringColumns(i))
              if (i != clusteringColumns.size - 1) {
                builder.append(",")
              }
            }
          }
        }
        builder.append("))")

        session =>
          session.execute(s"CREATE KEYSPACE IF NOT EXISTS $keyspace " +
            s"WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3 }")
          session.execute(builder.toString())
      }
    } catch {
      //      case e:NoSuchElementException => throw new IOException("To create a table, fields definition need to be provided")
      case e: NoSuchElementException => e.printStackTrace()
    }
  }
}
