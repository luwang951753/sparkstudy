package com.zysy.bigdata.practice

import java.text.SimpleDateFormat
import java.util.Locale
import java.util.concurrent.TimeUnit

import com.esri.core.geometry.{GeometryEngine, Point, SpatialReference}
import com.zysy.bigdata.sql.EitherTest.process
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{Row, SparkSession}

import scala.io.Source

object TaxiAnalysisRunner {

  def main(args: Array[String]): Unit = {

    // 1. 创建 SparkSession
    val spark = SparkSession.builder()
      .master("local[6]")
      .appName("taxi")
      .getOrCreate()

    // 2. 导入函数和隐式转换
    import spark.implicits._
    import org.apache.spark.sql.functions._

    // 3. 读取文件
    val taxiRaw = spark.read
      .option("header", value = true)
      .csv("dataset/half_trip.csv")

//    taxiRaw.show()
//    taxiRaw.printSchema()

    // 4. 数据转换和清洗
    val taxiParsed = taxiRaw.rdd.map(safe(parse))
    val taxiGood = taxiParsed.filter( either => either.isLeft )
      .map( either => either.left.get )
      .toDS()



    // 5. 过滤行程无效的数据
    val hours = (pickUp: Long, dropOff: Long) => {
      val duration = dropOff - pickUp
      TimeUnit.HOURS.convert(duration, TimeUnit.MILLISECONDS)
    }
    val hoursUDF = udf(hours)



    taxiGood.groupBy(hoursUDF($"pickUpTime", $"dropOffTime").as("duration"))
      .count()
      .sort("duration")
      .show()

    spark.udf.register("hours", hours)
    val taxiClean = taxiGood.where("hours(pickUpTime, dropOffTime) BETWEEN 0 AND 3")
    taxiClean.show()


    val geoJson = Source.fromFile("dataset/nyc-borough-boundaries-polygon.geojson").mkString
    val features = FeatureExtraction.parseJson(geoJson)

    val areaSortedFeatures = features.features.sortBy(feature => {
      (feature.properties("boroughCode"), - feature.getGeometry.calculateArea2D())
    })


    //动机: Geometry 对象数组相对来说是一个小数据集, 后续需要使用 Spark 来进行计算, 将 Geometry 分发给每一个 Executor 会显著减少 IO 通量
    val featuresBc = spark.sparkContext.broadcast(areaSortedFeatures)


    //创建 UDF, 接收每个出租车数据的下车经纬度, 转为行政区信息, 以便后续实现功能
    val boroughLookUp = (x: Double, y: Double) => {
      val features: Option[Feature] = featuresBc.value.find(feature => {
        GeometryEngine.contains(feature.getGeometry, new Point(x, y), SpatialReference.create(4326))
      })
      features.map(feature => {
        feature.properties("borough")
      }).getOrElse("NA")
    }

    val boroughUDF = udf(boroughLookUp)

    taxiClean.groupBy(boroughUDF('dropOffX, 'dropOffY))
      .count()
      .show()










  }

  /**
    * 包裹转换逻辑, 并返回 Either
    */
  def safe[P, R](f: P => R): P => Either[R, (P, Exception)] = {
    new Function[P, Either[R, (P, Exception)]] with Serializable {
      override def apply(param: P): Either[R, (P, Exception)] = {
        try {
          Left(f(param))
        } catch {
          case e: Exception => Right((param, e))
        }
      }
    }
  }


  /**
    * 将时间类型数据转为时间戳, 方便后续的处理
    * @param row 行数据, 类型为 RichRow, 以便于处理空值
    * @param field 要处理的时间字段所在的位置
    * @return 返回 Long 型的时间戳
    */
  def parseTime(row: RichRow, field: String): Long = {
    val pattern = "yyyy-MM-dd HH:mm:ss"
    val formatter = new SimpleDateFormat(pattern, Locale.ENGLISH)

    val timeOption = row.getAs[String](field)
    timeOption.map( time => formatter.parse(time).getTime )
      .getOrElse(0L)
  }

  /**
    * 将字符串标识的 Double 数据转为 Double 类型对象
    * @param row 行数据, 类型为 RichRow, 以便于处理空值
    * @param field 要处理的 Double 字段所在的位置
    * @return 返回 Double 型的时间戳
    */
  def parseLocation(row: RichRow, field: String): Double = {
    row.getAs[String](field).map( loc => loc.toDouble ).getOrElse(0.0D)
  }


  def parse(row: Row): Trip = {
    // 通过使用转换方法依次转换各个字段数据
    val row = new RichRow(row)
    val license = row.getAs[String]("hack_license").orNull
    val pickUpTime = parseTime(row, "pickup_datetime")
    val dropOffTime = parseTime(row, "dropoff_datetime")
    val pickUpX = parseLocation(row, "pickup_longitude")
    val pickUpY = parseLocation(row, "pickup_latitude")
    val dropOffX = parseLocation(row, "dropoff_longitude")
    val dropOffY = parseLocation(row, "dropoff_latitude")

    // 创建 Trip 对象返回
    Trip(license, pickUpTime, dropOffTime, pickUpX, pickUpY, dropOffX, dropOffY)
  }


}

/**
  * 代表一个行程, 是集合中的一条记录
  * @param license 出租车执照号
  * @param pickUpTime 上车时间
  * @param dropOffTime 下车时间
  * @param pickUpX 上车地点的经度
  * @param pickUpY 上车地点的纬度
  * @param dropOffX 下车地点的经度
  * @param dropOffY 下车地点的纬度
  */
case class Trip(
                 license: String,
                 pickUpTime: Long,
                 dropOffTime: Long,
                 pickUpX: Double,
                 pickUpY: Double,
                 dropOffX: Double,
                 dropOffY: Double
               )
class RichRow(row: Row) {

  def getAs[T](field: String): Option[T] = {
    if (row.isNullAt(row.fieldIndex(field)) || StringUtils.isBlank(row.getAs[String](field))) {
      None
    } else {
      Some(row.getAs[T](field))
    }
  }
}
