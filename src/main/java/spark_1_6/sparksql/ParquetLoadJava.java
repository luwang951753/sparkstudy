package spark_1_6.sparksql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.List;

public class ParquetLoadJava {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .set("spark.driver.host", "localhost")
                .setAppName("RDD2DataFrameReflectionJava");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        // 读取Parquet文件中的数据，创建一个DataFrame
        Dataset<Row> usersDF = sqlContext.read().parquet(
                "D://idea//project//sparkstudy//datas//users.parquet");

        // 将DataFrame注册为临时表，然后使用SQL查询需要的数据
        usersDF.registerTempTable("users");
        Dataset<Row> userNamesDF = sqlContext.sql("select name from users");

        // 对查询出来的DataFrame进行transformation操作，处理数据，然后打印出来
        List<String> userNames = userNamesDF.javaRDD().map(new Function<Row, String>() {

            private static final long serialVersionUID = 1L;

            @Override
            public String call(Row row) throws Exception {
                return "Name: " + row.getString(0);
            }

        }).collect();

        for(String userName : userNames) {
            System.out.println(userName);
        }
    }


}
