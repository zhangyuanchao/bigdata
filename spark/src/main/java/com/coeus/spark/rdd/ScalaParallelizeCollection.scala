package com.coeus.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}

object ScalaParallelizeCollection {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("ScalaParallelizeCollection").setMaster("local");
        val sc = new SparkContext(conf);
        val arr = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        val numberRDD = sc.parallelize(arr, 3);
        val sum = numberRDD.reduce(_ + _)
        println(sum)
    }

}
