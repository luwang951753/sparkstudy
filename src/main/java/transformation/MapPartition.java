package transformation;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.*;

/**
 * @author lj
 * @createDate 2019/8/15 16:08
 **/
public class MapPartition {

    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf()
                .setAppName("mapPartition")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 准备一下模拟数据
        List<String> studentNames = Arrays.asList("张三", "李四", "王二", "麻子");
        JavaRDD<String> studentNamesRDD = sc.parallelize(studentNames, 2);


        final Map<String, Double> studentScoreMap = new HashMap<String, Double>();
        studentScoreMap.put("张三", 278.5);
        studentScoreMap.put("李四", 290.0);
        studentScoreMap.put("王二", 301.0);
        studentScoreMap.put("麻子", 205.0);

        // mapPartitions
        // 类似map，不同之处在于，map算子，一次就处理一个partition中的一条数据
        // mapPartitions算子，一次处理一个partition中所有的数据

        // 推荐的使用场景
        // 如果你的RDD的数据量不是特别大，那么建议采用mapPartitions算子替代map算子，可以加快处理速度
        // 但是如果你的RDD的数据量特别大，比如说10亿，不建议用mapPartitions，可能会内存溢出

//        JavaRDD<Double> studentScoresRDD = studentNamesRDD.mapPartitions(new FlatMapFunction<Iterator<String>, Double>() {
//            @Override
//            public Iterator<Double> call(Iterator<String> iterator) throws Exception {
//                List<Double> studentScoreList = new ArrayList<Double>();
//
//                while (iterator.hasNext()) {
//                    String studentName = iterator.next();
//                    Double studentScore = studentScoreMap.get(studentName);
//                    studentScoreList.add(studentScore);
//                }
//
//                return studentScoreList.iterator();
//            }
//        });
//
//        for(Double studentScore: studentScoresRDD.collect()) {
//            System.out.println(studentScore);
//        }
//
//        sc.close();
        

        JavaRDD<String> studentWithClassRDD = studentNamesRDD.mapPartitionsWithIndex(

                new Function2<Integer, Iterator<String>, Iterator<String>>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Iterator<String> call(Integer index, Iterator<String> iterator)
                            throws Exception {
                        List<String> studentWithClassList = new ArrayList<String>();

                        while(iterator.hasNext()) {
                            String studentName = iterator.next();
                            String studentWithClass = studentName + "_" + (index + 1);
                            studentWithClassList.add(studentWithClass);
                        }

                        return studentWithClassList.iterator();
                    }

                }, true);

        for(String studentWithClass : studentWithClassRDD.collect()) {
            System.out.println(studentWithClass);
        }

        sc.close();



    }
}
