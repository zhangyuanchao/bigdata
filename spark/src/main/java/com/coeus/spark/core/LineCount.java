package com.coeus.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

/**
 * 统计每行出现的次数
 */
public class LineCount {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("LineCount").setMaster("local");
        // 创建JavaSparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 创建初始RDD
        JavaRDD<String> lines = sc.textFile("C:\\study\\hello.txt");

        // 对lines RDD执行mapToPair算子，将每一行映射成(line, 1)的这种key-value对的格式
        // 然后后面才能统计每一行出现的次数
        JavaPairRDD<String, Integer> pairs = lines.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });

        // 对pairs RDD执行reduceByKey算子，统计出每一行出现的次数
        JavaPairRDD<String, Integer> lineCounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        // 执行一个action操作
        lineCounts.foreach(new VoidFunction<Tuple2<String, Integer>>() {

            @Override
            public void call(Tuple2<String, Integer> t) throws Exception {
                System.out.println(t._1 + " appears" + t._2 + " times");
            }
        });

        // 关闭JavaSparkContext
        sc.close();

    }
}
