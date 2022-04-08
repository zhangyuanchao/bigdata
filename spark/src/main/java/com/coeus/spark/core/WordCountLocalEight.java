package com.coeus.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * 使用Java8编写wordcount
 */
public class WordCountLocalEight {
    SparkConf conf = new SparkConf().setMaster("local").setAppName("WordCountLocalEight");
    JavaSparkContext sc = new JavaSparkContext(conf);
    JavaRDD<String> lines = sc.textFile("C:\\study\\DORRIT.txt", 3);
    JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
    JavaPairRDD<String, Integer> pairs = words.mapToPair(word -> new Tuple2<>(word, 1));
    JavaPairRDD<String, Integer> wordCounts = pairs.reduceByKey((x, y) -> x + y);

    JavaPairRDD<Integer, String> count = wordCounts.mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1));

    JavaPairRDD<Integer, String> sort = count.sortByKey(false);
    JavaPairRDD<String, Integer> result = sort.mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1));
    // result.saveAsTextFile("E:\\result8");
}
