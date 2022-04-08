package com.coeus.spark.transformation

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object TransformationS {

    private var conf = new SparkConf().setMaster("local").setAppName("TransformationS");
    private var sc = new SparkContext(conf);

    def main(args: Array[String]): Unit = {
        // map();
        // flatMap();
        // mapPartitions();
        mapPartitionsWithIndex();
    }

    def map() : Unit = {
        var names = List("渡厄", "渡劫", "渡难");
        var listRDD  = sc.parallelize(names);
        var nameRDD = listRDD.map(name => "Hello " + name);
        nameRDD.foreach(name => println(name));
    }

    def flatMap() : Unit = {
        var names = List("明教教主:张无忌", "少林方丈:空闻大师", "武当掌门:张三丰");
        var listRDD = sc.parallelize(names);
        var nameRDD = listRDD.flatMap(name => name.split(":")).map(name => "Hello" + name);
        nameRDD.foreach(name => println(name));
    }

    def mapPartitions() : Unit = {
        var nums = List(1, 2, 3, 4, 5, 6);
        var listRDD = sc.parallelize(nums, 2);
        var numRDD = listRDD.mapPartitions(iterator => {
            var l : ListBuffer[String] = ListBuffer();
            while (iterator.hasNext) {
                l.append("s" + iterator.next())
            }
            l.toIterator;
        })

        numRDD.foreach(num => println(num));
    }

    def mapPartitionsWithIndex() : Unit = {
        var nums = List(1, 2, 3, 4, 5, 6);
        var listRDD = sc.parallelize(nums, 2);
        var numRDD = listRDD.mapPartitionsWithIndex((index, iterator) => {
            var l : ListBuffer[String] = ListBuffer();
            while (iterator.hasNext) {
                l.append(index + "_" + iterator.next())
            }
            l.iterator;
        });

        numRDD.foreach(num => println(num));
    }

}
