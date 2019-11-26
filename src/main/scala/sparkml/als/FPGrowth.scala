package sparkml.als

import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

case class UserAction1(user: String, item: Int, action: String, timestamp: String, num: Int)

object FPGrowth {

  val ITEM_COUNT: Int = 1 // 大于这个数量
  val path = "D:/10.1-10.11/fpg/3"

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[12]").setAppName("FPG").set("spark.executor.memory", "10g")
    val sc = SparkContext.getOrCreate(conf)
    val spark = SparkSession.builder.getOrCreate()
    sc.setLogLevel("ERROR")

    //读取行为日志
    val actionData = sc.textFile("D:\\BigData\\Workspace\\Spark_Test\\SparkALSScalamaster\\src\\main\\scala\\als\\ActionData.txt")
    // count:1162490
    var buyActionsRDD = actionData.map(_.split("\\001")
    match { case Array(user, item, action, timestamp, num) =>
        UserAction1(user.toString, item.toInt, action.toString,
          timestamp.toString, num.toInt)
    }).filter(x => x.action == "buy") //  count:60550

    val buyItem = buyActionsRDD.map(x => x.item).distinct.count()

    println("buyItem",buyItem) // 4

    // 购买数量超过阈值的
    var userItems = buyActionsRDD.map(x => (x.user, x.item)).groupByKey().map(_._2).filter(x => x.size > ITEM_COUNT) //count:10804
    userItems = userItems.sortBy(x => x.size, ascending = false)
    userItems foreach println
    //userItems.repartition(1).saveAsTextFile(path + "-orl")

    // userItems的格式：Array[Iterable[Int]] = Array(CompactBuffer(1600, 3770, 2758, 6681, 917), ......
    // 整理成： Array[String] = Array(1600 3770 2758 6681 917,
    val listRDD = userItems.map(x =>
      x.toArray.toList.toString().substring(4)
        .replace("(", "")
        .replace(")", "")
        .replace(", ", " "))

    // 格式：Array[Array[String]] = Array(Array(1600, 3770, 2758, 6681, 917),
    val transactions: RDD[Array[String]] = listRDD.map(
      s => s.trim.split(' ').distinct)
    println("orl-count", transactions.count())

    // setMinSupport 支持度support：项集在总项集里面出现的次数。
    // minConfidence 置信度confidence：包含X的项集中，Y出现的概率。u(X,Y)/u(x)。
    val fpg = new FPGrowth().setMinSupport(0.6).setNumPartitions(2)

    val model = fpg.run(transactions)

    val lst1 = new ListBuffer[String]
    model.freqItemsets.collect().foreach { itemset =>
      val items = itemset.items
      if (items.length > 1) {
        val str = itemset.items.mkString("[", ",", "]") + "," + itemset.freq
        lst1 += str
      }
            println(s"${itemset.items.mkString("[", ",", "]")},${itemset.freq}")
    }

//    sc.makeRDD(lst1).repartition(1).saveAsTextFile(path + "-recomm-item-freq")

    val lst2 = new ListBuffer[String]
    val minConfidence = 0.8
    model.generateAssociationRules(minConfidence).collect().foreach { rule =>
      val str2 = rule.antecedent.mkString("[", ",", "]") + " " + rule.consequent.mkString("[", ",", "]") + " " + rule.confidence
      lst2 += str2
            println(s"${rule.antecedent.mkString("[", ",", "]")}=> " +
              s"${rule.consequent.mkString("[", ",", "]")},${rule.confidence}")
    }
//    sc.makeRDD(lst2).repartition(1).saveAsTextFile(path + "-confidence")
  }
}
