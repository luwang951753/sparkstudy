package sparkml

import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.DecisionTreeClassificationModel
import org.apache.spark.ml.classification.DecisionTreeClassifier
/**
  * Created by zhao-chj on 2018/8/31.
  */
object DescitionTreeML {
  def main(args: Array[String]): Unit = {
    val spark:SparkSession=SparkSession.builder()
      .appName("DescitionTreeML").master("local[2]")
    .config("spark.driver.host", "localhost").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    // Load the data stored in LIBSVM format as a DataFrame.
    val data = spark.read.format("libsvm").load("D:\\idea\\project\\sparkstudy\\datas\\sample_libsvm_data.txt")
    data.show(5)
    // Index labels, adding metadata to the label column.
    // Fit on whole dataset to include all labels in index.
    val labelIndexer= new StringIndexer().setInputCol("label").setOutputCol("indexedLabel").fit(data)
    val featureIndexer=new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").setMaxCategories(4).fit(data)
    // Split the data into training and test sets (30% held out for testing).
    val Array(trainingset,testset)= data.randomSplit(Array(0.7, 0.3))
    // Train a DecisionTree model.
    val dt = new DecisionTreeClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("indexedFeatures")

    // Convert indexed labels back to original labels.
    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)
    // Chain indexers and tree in a Pipeline.
    val pipeline = new Pipeline()
      .setStages(Array(labelIndexer, featureIndexer, dt, labelConverter))
    // Train model. This also runs the indexers.
    val model = pipeline.fit(trainingset)

    // Make predictions.
    val predictions = model.transform(testset)

    // Select example rows to display.
    predictions.select("predictedLabel", "label", "features").show(5)
  }
}