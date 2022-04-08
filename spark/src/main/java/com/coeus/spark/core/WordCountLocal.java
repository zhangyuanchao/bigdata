package com.coeus.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * 本地测试的wordcount程序
 */
public class WordCountLocal {

    public static void main(String[] args) {
        // 编写Spark应用程序
        // 本地执行是可以直接在IDE中的main方法中执行的

        // 第一步: 创建SparkConf对象, 设置Spark应用的配置信息
        // 使用setMaster可以设置Spark应用程序要连接的Spark集群的master节点的url; 但是如果设置为local, 则代表在本地运行
        SparkConf conf = new SparkConf().setAppName("WordCountLocal").setMaster("local");
        // 第二步: 创建JavaSparkContext对象
        // 在Spark中, SparkContext是Spark所有功能的一个入口; 无论是Java、Scala甚至是python编写, 都必须要有一个SparkContext;
        // 它的主要作用包括初始化Spark应用程序所需的一些核心组件, 包括调度器(DAGSchedule、TaskScheduler), 还会到Spark Master节点进行注册...
        // 一句话, SparkContext是Spark应用中, 可以说是最最重要的一个对象
        // 但是, 在Spark中编写不同类型的Spark应用程序, 使用的SparkContext是不同的
        // 如果使用Scala, 使用的就是原生的SparkContext对象; 但是如果使用Java, 那么就是JavaSparkContext对象;
        // 如果是开发Spark SQL程序, 那么就是SQLContext、HiveContext; 如果是开发Spark Streaming程序, 那么就是它独有的SparkContext; 依此类推
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 第三步: 要针对输入源(HDFS文件、本地文件...)创建一个初始的RDD
        // 输入源中的数据会打散, 分配到RDD的每个partition中, 从而形成一个初始的分布式的数据集
        // 这里呢, 因为是本地测试, 所以呢, 就是针对本地文件
        // SparkContext中, 用于根据文件类型的输入源创建RDD的方法, 叫做textFile()方法
        // 在Java中创建的普通RDD都叫做JavaRDD
        // 在这里, RDD中有元素这种概念; 如果是HDFS或者本地文件系统呢, 创建的RDD, 每一个元素就相当于文件中的一行
        JavaRDD<String> lines = sc.textFile("C:\\study\\DORRIT.txt");

        // 第四步: 对初始RDD进行transformation操作, 也就是一些计算操作
        // 通常操作会通过创建function, 并配合RDD的map、flatMap等算子来执行function; 通常如果比较简单, 则创建指定function的匿名内部类
        // 但是如果function比较复杂, 则会创建一个单独的类, 作为这个function接口的实现类

        // 先将每一行拆分成单个的单词
        // FlatMapFunction有两个泛型参数, 分别代表了输入和输出类型
        // 在这里, 输入是String类型, 因为是一行一行的文本; 输出, 其实也是String类型, 因为是每一个单词
        // 这里先简要介绍flatMap算子的作用, 其实就是将RDD的一个元素拆分成一个或多个元素
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" ")).iterator();
            }
        });
        // 第五步: 接着, 需要将每个单词映射成(单词, 1)的这种格式, 因为只有这样, 后面才能根据单词作为key, 来进行每个单词出现次数的累加
        // mapToPair其实就是将每个元素映射为一个(v1, v2)这样的tuple2这样的类型
        // mapToPair这个算子要求的是与PairFunction配合使用, 第一个泛型参数表示的是输入类型, 第二个和第三个泛型参数代表的是Tuple2的第一个和第二个值的类型
        // JavaPairRDD的两个泛型参数, 分别代表了tuple元素得第一个值和第二个值的类型
        JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                // return Tuple2.apply(word, 1);
                return new Tuple2<String, Integer>(word, 1);
            }
        });

        // 第六步: 需要以单词作为key, 统计每个单词出现的次数
        // 这里需要使用reduceByKey这个算子, 对每个key对应的value都进行reduce操作
        // 比如JavaPairRDD中有几个元素分别为(hello, 1) (hello, 1) (world, 1)
        // reduce操作相当于是把第一个值和第二个值进行计算, 然后再把结果和第三个值进行计算
        JavaPairRDD<String, Integer> wordCounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        // 到这里, 通过几个Spark算子操作, 已经统计出了单词出现的次数
        // 之前使用的flatMap、mapToPair、reduceByKey这种操作, 都叫做transformation操作
        // 一个Spark应用中, 只有transformation操作是不行的, 是不会执行的, 必须要有一种叫做action
        // 最后可以使用一种叫做action操作的, 比如说foreach来触发程序的执行

//        wordCounts.foreach(new VoidFunction<Tuple2<String, Integer>>() {
//            @Override
//            public void call(Tuple2<String, Integer> wordCount) throws Exception {
//                System.out.println(wordCount._1 + "appeared:" + wordCount._2 + " times.");
//            }
//        });
//        sc.close();
    }
}
