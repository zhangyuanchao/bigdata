package com.coeus.spark.action

import org.apache.spark.{SparkConf, SparkContext}

object ActionS {

    private var conf = new SparkConf().setMaster("local").setAppName("TransformationS");
    private var sc = new SparkContext(conf);

    def main(args: Array[String]): Unit = {
        // reduce();
        // reduceByKey();
        union();
    }

    def reduce() : Unit = {
        var list = List(1, 2, 3, 4, 5, 6);
        var listRDD = sc.parallelize(list);
        var result = listRDD.reduce(_+_);
        println(result)
    }

    def reduceByKey() : Unit = {
        var list = List(("武当", 99), ("少林", 97), ("武当", 89), ("少林", 77));
        var listRDD = sc.parallelize(list);
        var pairRDD = listRDD.reduceByKey(_ + _);
        pairRDD.foreach(t => println("门派:" + t._1 + ", 人数:" + t._2));
    }

    def union() : Unit = {
        var list1 = List(1, 2, 3, 4);
        var list2 = List(3, 4, 5, 6)
        var list1RDD = sc.parallelize(list1);
        var list2RDD = sc.parallelize(list2);
        list1RDD.union(list2RDD).foreach(println(_));
    }

    def groupByKey() : Unit = {
        val list = List(("武当", "张三丰"), ("峨眉", "灭绝师太"), ("武当", "宋青书"), ("峨眉", "周芷若"))
        var listRDD = sc.parallelize(list);
        var groupRDD = listRDD.groupByKey();
        groupRDD.foreach(t => {
            val menpai = t._1
            val iterator = t._2.iterator
            var people = ""
            while (iterator.hasNext) people = people + iterator.next + " "
            println("门派:" + menpai + "人员:" + people)
        });
    }

}
