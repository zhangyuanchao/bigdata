package com.coeus.spark.core

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args : Array[String]) {
//    val conf = new SparkConf().setAppName("WordCount").set("spark.testing.memory","2147480000");
//    val sc = new SparkContext(conf);
//
//    val lines = sc.textFile("hdfs://nn-services/spark/DORRIT.txt", 1);
//    val words = lines.flatMap{line => line.split(" ")};
//    val pairs= words.map{word => (word, 1)};
//    val wordCounts = pairs.reduceByKey{_ + _};
//    // wordCounts.foreach{wordCount => println(wordCount._1 + " appeared " + wordCount._2 + " times")}
//    var sortRdd = wordCounts.sortBy(t => t._2, false);
//    sortRdd.saveAsTextFile("E:\\result");
    var conf = new SparkConf().setAppName("WordCount").set("spark.testing.memory","2147480000");
    var sc = new SparkContext(conf);
    var lines = sc.textFile("hdfs://nn-services/spark/DORRIT.txt", 1);
    var words = lines.flatMap(line => line.split(" "));
    var pairs = words.map(word => (word, 1));
    var wordCounts = pairs.reduceByKey(_+_);

  }
}
