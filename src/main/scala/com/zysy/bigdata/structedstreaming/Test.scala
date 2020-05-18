package com.zysy.bigdata.structedstreaming

object Test {

  class  SparkConfNew(loadDefaults: Boolean){

    def this() = this(true)

    if (loadDefaults){
      println("aaaaaaa")
    }
  }

  def main(args: Array[String]): Unit = {
     val conf = new SparkConfNew()
    /*val aa : Option[String] = None
    val b = aa.getOrElse{
      "aaa"
    }
    println(b)*/
  }

  

}
