package com.zysy.bigdata.practice

import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization.{read, write}


case class Product(name: String, price: Double)
object JsonTest {

  def main(args: Array[String]): Unit = {


    val product =
      """
        |{"name":"Toy","price":35.35}
      """.stripMargin

    // 使用序列化 API 之前, 要先导入代表转换规则的 formats 对象隐式转换
    implicit val formats = Serialization.formats(NoTypeHints)

    //print(parse(product) \ "name" )

    // 可以解析 JSON 为对象
    val obj: Product = parse(product).extract[Product]

    // 可以将对象序列化为 JSON
    val str: String = compact(render(parse(product)))

    // 可以使用序列化的方式来将 JSON 字符串反序列化为对象
    val obj1 = read[Product](product)

    // 可以使用序列化的方式将对象序列化为 JSON 字符串
    val str1 = write(Product("电视", 10.5))
  }

}
