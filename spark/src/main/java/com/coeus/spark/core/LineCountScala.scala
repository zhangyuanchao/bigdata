package com.coeus.spark.core

import org.apache.spark.{SparkConf, SparkContext}

object LineCountScala {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("LineCountScala").setMaster("local");
        val sc = new SparkContext(conf);
        val lines = sc.textFile("C:\\study\\hello.txt");
        val pairs = lines.map(line => (line, 1));
        val lineCount = pairs.reduceByKey(_ + _);

    }

}
