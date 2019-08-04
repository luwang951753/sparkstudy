package spark_1_6.sparksql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

public class FDtest {

    public static void main(String[] args) {
        SparkConf  conf = new SparkConf().setAppName("datest");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        Dataset<Row> df = sqlContext.read().json("hdfs://mini1:9000/students.json");
        df.printSchema();
        df.select("name").show();
        df.select(df.col("name"), df.col("age").plus(1)).show();
        df.filter(df.col("age").gt(21)).show();
        df.groupBy("age").count().show();


    }
}
