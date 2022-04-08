package com.coeus.spark.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.Arrays;
import java.util.List;

public class ParallelizeCollection {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("ParallelizeCollection")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> numberRDD = sc.parallelize(numbers);
        Integer sum = numberRDD.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer first, Integer second) throws Exception {
                return first + second;
            }
        });
        System.out.println(sum);
        sc.close();
    }
}
