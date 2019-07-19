package transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Transformation {

    public static void main(String[] args) {
        map();

    }

    public static void map(){
        SparkConf conf = new SparkConf().setAppName("test").setMaster("local");
        JavaSparkContext sc  = new JavaSparkContext(conf);

        List<Integer> list = Arrays.asList(1,2,3,4,5);

        JavaRDD<Integer> datas = sc.parallelize(list);

        JavaRDD<Integer> result = datas.map(new Function<Integer, Integer>() {
            public Integer call(Integer v1) throws Exception {
                return v1 * 2;
            }
        });
        result.foreach(new VoidFunction<Integer>() {
            public void call(Integer integer) throws Exception {
                System.out.println(integer + "");
            }
        });
        //sc.close();




    }
}
