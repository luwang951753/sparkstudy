package transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.RuntimeConfig;
import org.apache.spark.sql.SparkSession;
import scala.Option;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class Transformation {

    public static void main(String[] args) {
        map();
        //join();
        //cogroup();



    }

    public static void map(){
        //2.0之前
        /*SparkConf conf = new SparkConf().setAppName("test").setMaster("local");
        JavaSparkContext sc  = new JavaSparkContext(conf);*/

        //2.0之后
        SparkSession session = SparkSession.builder().
                appName("test").
                master("local").
                config("spark.driver.host", "localhost").
                getOrCreate();
        JavaSparkContext sc = JavaSparkContext.fromSparkContext(session.sparkContext());

        List<Integer> list = Arrays.asList(1,2,3,4,5);

       // JavaRDD<Integer> datas = sc.parallelize(list);
        JavaRDD<Integer> datas = sc.parallelize(list);

//        JavaRDD<Integer> result = datas.map(new Function<Integer, Integer>() {
//            public Integer call(Integer v1) throws Exception {
//                return v1 * 2;
//            }
//        });
//        result.foreach(new VoidFunction<Integer>() {
//            public void call(Integer integer) throws Exception {
//                System.out.println(integer + "");
//            }
//        });

//        JavaRDD<String> result = datas.flatMap(new FlatMapFunction<Integer, String>() {
//            @Override
//            public Iterator<String> call(Integer integer) throws Exception {
//                List<String> list = new ArrayList<>();
//                list.add(Integer.toString(integer));
//                list.add(Integer.toString(9));
//                list.add(Integer.toString(10));
//                return list.iterator();
//            }
//        });
//
//        result.foreach(new VoidFunction<String>() {
//            @Override
//            public void call(String s) throws Exception {
//                System.out.println(s);
//            }
//        });

//        JavaRDD<String> result = datas.flatMapToPair(new PairFlatMapFunction<Integer, K2, V2>() {
//            @Override
//            public Iterator<Tuple2<K2, V2>> call(Integer integer) throws Exception {
//                return null;
//            }
//        })
//
//        result.foreach(new VoidFunction<String>() {
//            @Override
//            public void call(String s) throws Exception {
//                System.out.println(s);
//            }
//        });

        sc.close();






    }



    /**
     * join案例：打印学生成绩
     */
    private static void join() {
        // 创建SparkConf
        SparkSession session = SparkSession.builder().
                appName("test").
                master("local").
                config("spark.driver.host", "localhost").
                getOrCreate();
        JavaSparkContext sc = JavaSparkContext.fromSparkContext(session.sparkContext());

        // 模拟集合
        List<Tuple2<Integer, String>> studentList = Arrays.asList(
                new Tuple2<Integer, String>(1, "leo"),
                new Tuple2<Integer, String>(2, "jack"),
                new Tuple2<Integer, String>(3, "tom"));

        List<Tuple2<Integer, Integer>> scoreList = Arrays.asList(
                new Tuple2<Integer, Integer>(1, 100),
                new Tuple2<Integer, Integer>(2, 90),
                new Tuple2<Integer, Integer>(3, 60),
                new Tuple2<Integer, Integer>(1, 70),
                new Tuple2<Integer, Integer>(2, 80),
                new Tuple2<Integer, Integer>(3, 50));

        // 并行化两个RDD
        JavaPairRDD<Integer, String> students = sc.parallelizePairs(studentList);
        JavaPairRDD<Integer, Integer> scores = sc.parallelizePairs(scoreList);

        // 使用join算子关联两个RDD
        // join以后，还是会根据key进行join，并返回JavaPairRDD
        // 但是JavaPairRDD的第一个泛型类型，之前两个JavaPairRDD的key的类型，因为是通过key进行join的
        // 第二个泛型类型，是Tuple2<v1, v2>的类型，Tuple2的两个泛型分别为原始RDD的value的类型
        // join，就返回的RDD的每一个元素，就是通过key join上的一个pair
        // 什么意思呢？比如有(1, 1) (1, 2) (1, 3)的一个RDD
        // 还有一个(1, 4) (2, 1) (2, 2)的一个RDD
        // join以后，实际上会得到(1 (1, 4)) (1, (2, 4)) (1, (3, 4))
        JavaPairRDD<Integer, Tuple2<String, Integer>> studentScores = students.join(scores);

        // 打印studnetScores RDD
        studentScores.foreach(

                new VoidFunction<Tuple2<Integer,Tuple2<String,Integer>>>() {

                    private static final long serialVersionUID = 1L;


                    public void call(Tuple2<Integer, Tuple2<String, Integer>> t)
                            throws Exception {
                        System.out.println("student id: " + t._1);
                        System.out.println("student name: " + t._2._1);
                        System.out.println("student score: " + t._2._2);
                        System.out.println("===============================");
                    }

                });

        // 关闭JavaSparkContext
        sc.close();
    }

    /**
     * cogroup案例：打印学生成绩
     */
    private static void cogroup() {
        // 创建SparkConf
        SparkSession session = SparkSession.builder().
                appName("test").
                master("local").
                config("spark.driver.host", "localhost").
                getOrCreate();
        JavaSparkContext sc = JavaSparkContext.fromSparkContext(session.sparkContext());

        // 模拟集合
        List<Tuple2<Integer, String>> studentList = Arrays.asList(
                new Tuple2<Integer, String>(1, "leo"),
                new Tuple2<Integer, String>(2, "jack"),
                new Tuple2<Integer, String>(3, "tom"));

        List<Tuple2<Integer, Integer>> scoreList = Arrays.asList(
                new Tuple2<Integer, Integer>(1, 100),
                new Tuple2<Integer, Integer>(2, 90),
                new Tuple2<Integer, Integer>(3, 60),
                new Tuple2<Integer, Integer>(1, 70),
                new Tuple2<Integer, Integer>(2, 80),
                new Tuple2<Integer, Integer>(3, 50));

        // 并行化两个RDD
        JavaPairRDD<Integer, String> students = sc.parallelizePairs(studentList);
        JavaPairRDD<Integer, Integer> scores = sc.parallelizePairs(scoreList);

        // cogroup与join不同
        // 相当于是，一个key join上的所有value，都给放到一个Iterable里面去了
        // cogroup，不太好讲解，希望大家通过动手编写我们的案例，仔细体会其中的奥妙
        JavaPairRDD<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> studentScores =
                students.cogroup(scores);

        // 打印studnetScores RDD
        studentScores.foreach(

                new VoidFunction<Tuple2<Integer,Tuple2<Iterable<String>,Iterable<Integer>>>>() {

                    private static final long serialVersionUID = 1L;


                    public void call(
                            Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> t)
                            throws Exception {
                        System.out.println("student id: " + t._1);
                        System.out.println("student name: " + t._2._1);
                        System.out.println("student score: " + t._2._2);
                        System.out.println("===============================");
                    }

                });

        // 关闭JavaSparkContext
        sc.close();
    }
}
