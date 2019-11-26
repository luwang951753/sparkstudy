package sparkml

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.SparkSession

/**
  * Created by zhao-chj on 2018/7/15.
  */
case class Rating(userId: Int, movieId: Int, rating: Float, timestamp: Long)
object MovieReconmentALS {
  def parseRating(str: String): Rating = {
    val fields = str.split("::")
    assert(fields.size == 4)
    Rating(fields(0).toInt, fields(1).toInt, fields(2).toFloat, fields(3).toLong)
  }

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .appName("SparkMlilb")
      .master("local[2]")
      .config("spark.driver.host", "localhost")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    val ratings = spark.read.textFile("D:\\idea\\project\\sparkstudy\\datas\\sample_movielens_ratings.txt")
      .map(x=>parseRating(x))
      .toDF()
    val Array(training, test) = ratings.randomSplit(Array(0.8, 0.2))

    // Build the recommendation model using ALS on the training data
    val als = new ALS()
      .setMaxIter(5)
      .setRegParam(0.01)
      .setUserCol("userId")
      .setItemCol("movieId")
      .setRatingCol("rating")
    val model = als.fit(training)

    // Evaluate the model by computing the RMSE on the test data
    val predictions = model.transform(test)

    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("prediction")
    val rmse = evaluator.evaluate(predictions)
    println(s"Root-mean-square error = $rmse")
    // Generate top 10 movie recommendations for each user为每个用户生成前10个电影推荐
    val userRecs = model.recommendForAllUsers(10)
    userRecs.show()
    // Generate top 10 user recommendations for each movie为每部电影生成前10个用户推荐
    val movieRecs = model.recommendForAllItems(10)
    movieRecs.show()
    // Generate top 10 movie recommendations for a specified set of users
    val users = ratings.select(als.getUserCol).distinct().limit(3)
    val userSubsetRecs = model.recommendForUserSubset(users, 10)
    userSubsetRecs.show()
    // Generate top 10 user recommendations for a specified set of movies
    val movies = ratings.select(als.getItemCol).distinct().limit(3)
    val movieSubSetRecs = model.recommendForItemSubset(movies, 10)
    movieSubSetRecs.show()
  }
}
