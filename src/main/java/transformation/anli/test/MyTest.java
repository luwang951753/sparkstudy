package transformation.anli.test;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.SparkSession;
import scala.Function1;
import scala.Tuple2;
import scala.collection.TraversableOnce;
import sun.awt.geom.AreaOp;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.Media.print;
import static sun.misc.Version.print;

/**
 * @author lj
 * @createDate 2019/8/28 17:36
 **/
public class MyTest {

    public static void main(String[] args) {
        SparkSession session = SparkSession.builder().
                master("local").
                appName("mytest").
                getOrCreate();

        JavaSparkContext sc = JavaSparkContext.fromSparkContext(session.sparkContext());
//        JavaRDD<String> stringJavaRDD = sc.textFile("D:\\work\\ideaproject\\sparkstudy\\datas\\data.txt", 4);
//
//        JavaRDD<String> rddflatmap = stringJavaRDD.flatMap(new FlatMapFunction<String, String>() {
//            @Override
//            public Iterator<String> call(String s) throws Exception {
//                String[] s1 = s.split(",");
//                List<String> result = new ArrayList<>(Arrays.asList(s1));
//                return result.iterator();
//            }
//        });
//
//        JavaPairRDD<String, Integer> result = rddflatmap.mapToPair((PairFunction<String, String, Integer>) s -> new Tuple2<>(s, 1))
//                .reduceByKey(Integer::sum);
//
//        JavaPairRDD<Integer, String> sortrdd = result.mapToPair(t -> new Tuple2<>(t._2, t._1));
//        /*sortrdd.foreach((VoidFunction<Tuple2<Integer, String>>) d -> {
//            System.out.println(d._1 + "===" + d._2);
//        });*/
//        JavaPairRDD<Integer, String> sorted = sortrdd.sortByKey(false);
//        sorted.foreach((VoidFunction<Tuple2<Integer, String>>) d -> {
//            System.out.println(d._1 + "===" + d._2);
//        });
//        JavaPairRDD<String, Integer> result1 = sorted.mapToPair(t -> new Tuple2<>(t._2, t._1));
//
//        result1.foreach(new VoidFunction<Tuple2<String, Integer>>() {
//            @Override
//            public void call(Tuple2<String, Integer> d) throws Exception {
//                System.out.println(d._1 + "===" + d._2);
//            }
//        });
        List<Tuple2<Integer, String>> studentList = Arrays.asList(
                new Tuple2<Integer, String>(1, "leo"),
                new Tuple2<Integer, String>(2, "jack"),
                new Tuple2<Integer, String>(3, "tom"),
                new Tuple2<Integer, String>(3, "A"),
                new Tuple2<Integer, String>(3, "B"));

        JavaPairRDD<Integer, String> re = sc.parallelizePairs(studentList);
        JavaPairRDD<Integer, String> re1 = re.reduceByKey((Function2<String, String, String>) (v1, v2) -> v2);
        List<Tuple2<Integer, String>> collect = re1.collect();
        collect.forEach(System.out::println);
        //collect.foreach((VoidFunction<Tuple2<Integer, String>>) System.out::println);

    }
}
