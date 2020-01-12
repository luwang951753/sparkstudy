package com.zysy.bigdata.sql

object EitherTest {

  val process = (b: Double) => {
    val a = 10.0
    a / b
  }

  def safe(function: Double => Double, b: Double): Either[Double, (Double, Exception)] = {
    try {
      val result = function(b)
      Left(result)
    } catch {
      case e: Exception => Right(b, e)
    }
  }

  val result = safe(process, 0)

  result match {
    case Left(r) => println(r)
    case Right((b, e)) => println(b, e)
  }

}
