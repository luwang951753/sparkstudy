package com.zysy.bigdata.util

import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime, OffsetDateTime, ZoneId}
import java.util.Date

object TimeUtil {

  def format(datetime: String, format1: String = "yyyy-MM-dd HH:mm:ss", format2: String = "yyyy-MM-dd HH:mm:ss"): String = {
    LocalDateTime.parse(datetime, DateTimeFormatter.ofPattern(format1))
      .format(DateTimeFormatter.ofPattern(format2))
  }

  def format_timestamp(timestamp: Long, format: String = "yyyy-MM-dd HH:mm:ss"): String = {
    LocalDateTime.ofEpochSecond(timestamp, 0, OffsetDateTime.now().getOffset)
      .format(DateTimeFormatter.ofPattern(format))
  }

  def today(format: String = "yyyy-MM-dd"): String = {
    LocalDate.now().format(DateTimeFormatter.ofPattern(format))
  }

  def tomorrow(format: String = "yyyy-MM-dd"): String = {
    plus_days(LocalDate.now().format(DateTimeFormatter.ofPattern(format)), 1, format)
  }

  def plus_days(today: String, n: Int = 1, format: String = "yyyy-MM-dd"): String = {
    LocalDate.parse(today, DateTimeFormatter.ofPattern(format))
      .plusDays(n).format(DateTimeFormatter.ofPattern(format))
  }

  def yesterday(format: String = "yyyy-MM-dd"): String = {
    LocalDate.now().plusDays(-1).format(DateTimeFormatter.ofPattern(format))
  }

  def date_timestamp(date: String, format: String = "yyyy-MM-dd"): Long = {
    val zone_id = ZoneId.systemDefault()
    LocalDate.parse(date, DateTimeFormatter.ofPattern(format)).atStartOfDay(zone_id).toEpochSecond
  }

  def datetime_timestamp(datetime: String, format: String = "yyyy-MM-dd HH:mm:ss"): Long = {
    val zone_id = ZoneId.systemDefault()
    LocalDateTime.parse(datetime, DateTimeFormatter.ofPattern(format)).atZone(zone_id).toEpochSecond
  }

  def time_diff(t1: Long, t2: Long): Long = {
    Math.abs(t1 - t2)
  }

  def time_diff(t1: String, t2: String, format: String = "yyyy-MM-dd HH:mm:ss"): Long = {
    val zone_offset = OffsetDateTime.now().getOffset
    val pattern = DateTimeFormatter.ofPattern(format)
    LocalDateTime.parse(t1, pattern).toEpochSecond(zone_offset) - LocalDateTime.parse(t2, pattern)
      .toEpochSecond(zone_offset)
  }

  def now_epoch_second(): Long = {
    LocalDateTime.now().atZone(ZoneId.systemDefault()).toEpochSecond
  }

  def now_second(): String = {
    LocalDateTime.now().atZone(ZoneId.systemDefault()).toEpochSecond.toString
  }

  def now():String = {
    format_timestamp(now_epoch_second())
  }

  //当前时间的后一分钟时间
  def times(time : String) :(String)= {
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd-HHmmss")
    val t = dateFormat.parse(time).getTime()
    var endtimes = 0L
    if(t%(60*1000) > 0L ){
      endtimes = t - t%(60*1000) + 60 * 1000
    }else {
      endtimes = t
    }
    val end = new SimpleDateFormat("yyyyMMdd-HHmmss").format(new Date(endtimes))
    end
  }

  def main(args: Array[String]): Unit = {
    println(now())
    println(now_second())
//    println(format_timestamp(1501745265,"yyyy-MM-dd"))
//    println(format_timestamp(1501745265,"yyyyMMdd-HH:mm:ss"))
  }
}
