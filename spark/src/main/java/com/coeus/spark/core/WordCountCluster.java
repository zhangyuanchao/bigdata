package com.coeus.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * 部署到集群的wordcount程序
 */
public class WordCountCluster {

    public static void main(String[] args) {

        // 如果要在Spark集群运行, 需要修改的只有两个地方:
        // 第一, 将SparkConf的setMaster()方法给删除掉, 默认它自己会去连接
        // 第二, 我们针对的不是本地文件了, 修改为HDFS上存储的文件

        // 实际执行步骤:
        // 1、将要计算的文件上传到HDFS
        // 2、对使用maven工程进行打包
        SparkConf conf = new SparkConf()
                .setAppName("WordCountCluster").set("spark.testing.memory","2147480000");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("hdfs://nn-services/spark/DORRIT.txt");
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" ")).iterator();
            }
        });

        JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                // return Tuple2.apply(word, 1);
                return new Tuple2<String, Integer>(word, 1);
            }
        });

        JavaPairRDD<String, Integer> wordCounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        wordCounts.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> wordCount) throws Exception {
                System.out.println(wordCount._1 + "appeared:" + wordCount._2 + " times.");
            }
        });
        sc.close();
    }
}
