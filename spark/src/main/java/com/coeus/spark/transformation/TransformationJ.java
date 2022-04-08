package com.coeus.spark.transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class TransformationJ {
    static SparkConf conf = null;
    static JavaSparkContext sc = null;

    static {
        conf = new SparkConf().setMaster("local").setAppName("TransformationJ");
        sc = new JavaSparkContext(conf);
    }

    public static void main(String[] args) {
        // mapSeven();
        // mapEight();

        // flatMapSeven();
        // flatMapEight();

        // mapPartitionsSeven();
        // mapPartitionsEight();

        // mapPartitionsWithIndexSeven();
        mapPartitionsWithIndexEight();
    }

    // java7编写map
    public static void mapSeven() {
        List<String> names = Arrays.asList("渡厄", "渡劫", "渡难");
        System.out.println(names.size());
        JavaRDD<String> listRDD = sc.parallelize(names);
        JavaRDD<String> nameRDD = listRDD.map(new Function<String, String>() {

            @Override
            public String call(String s) throws Exception {
                return "Hello7 " + s;
            }
        });

//        JavaRDD<String> nameRDD = listRDD.flatMap(new FlatMapFunction<String, String>() {
//            @Override
//            public Iterator<String> call(String s) throws Exception {
//                return Arrays.asList("Hello " + s).iterator();
//            }
//        });

        nameRDD.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });
    }

    // java7编写flatMap
    public static void flatMapSeven() {
        List<String> names = Arrays.asList("明教教主 张无忌", "少林方丈 空闻大师", "武当掌门 张三丰");
        JavaRDD<String> listRDD = sc.parallelize(names);
        JavaRDD<String> nameRDD = listRDD.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });
        nameRDD.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });
    }

    // java7编写mapPartitions
    public static void mapPartitionsSeven() {
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6);
        // 第二个参数代表这个RDD里面有2个分区
        JavaRDD<Integer> listRDD = sc.parallelize(list, 3);

        JavaRDD<Integer> numRDD = listRDD.mapPartitions(new FlatMapFunction<Iterator<Integer>, Integer>() {
            @Override
            public Iterator<Integer> call(Iterator<Integer> iterator) throws Exception {
                Integer sum = 0;
                while (iterator.hasNext()) {
                    sum += iterator.next();
                }
                return Arrays.asList(sum).iterator();
            }
        }, true);

        numRDD.foreach(new VoidFunction<Integer>() {

            @Override
            public void call(Integer s) throws Exception {
                System.out.println(s);
            }
        });

        Integer sum = numRDD.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer i1, Integer i2) throws Exception {
                return i1 + i2;
            }
        });
        System.out.println(sum);
    }

    // java7编写mapPartitionsWithIndex, mapPartitionsWithIndex方法需要指定preservesPartitioning参数,表示保留父RDD的分区信息
    public static void mapPartitionsWithIndexSeven() {
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6);
        JavaRDD<Integer> listRDD = sc.parallelize(list, 2);
        JavaRDD<String> indexRDD = listRDD.mapPartitionsWithIndex(new Function2<Integer, Iterator<Integer>, Iterator<String>>() {
            @Override
            public Iterator<String> call(Integer index, Iterator<Integer> iterator) throws Exception {
                ArrayList<String> array = new ArrayList<>();
                while(iterator.hasNext()) {
                    array.add(index + "_" + iterator.next());
                }
                return array.iterator();
            }
        }, true);

        indexRDD.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });
    }

    /**************************************************************************************/
    // java8编写map
    public static void mapEight() {
        List<String> names = Arrays.asList("渡厄", "渡劫", "渡难");
        System.out.println(names.size());
        JavaRDD<String> listRDD = sc.parallelize(names);
        JavaRDD<String> nameRDD = listRDD.map(l -> "Hello8 " + l);
        nameRDD.foreach(name -> System.out.println(name));
    }

    // java8编写flatMap
    public static void flatMapEight() {
        List<String> names = Arrays.asList("明教教主 张无忌", "少林方丈 空闻大师", "武当掌门 张三丰");
        JavaRDD<String> listRDD = sc.parallelize(names);
        JavaRDD<String> nameRDD = listRDD.flatMap(name -> Arrays.asList(name.split(" ")).iterator()).map(name -> "Hello " + name);
        nameRDD.foreach(name -> System.out.println(name));
    }

    // java8编写mapPartitions
    public static void mapPartitionsEight() {
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6);
        JavaRDD<Integer> listRDD = sc.parallelize(list, 2);
        JavaRDD<String> numRDD = listRDD.mapPartitions((iterator) -> {
            ArrayList<String> array = new ArrayList<>();
            while (iterator.hasNext()) {
                array.add("s" + iterator.next());
            }
            return array.iterator();
        });

        numRDD.foreach(num -> System.out.println(num));
    }

    // java8编写mapPartitionsWithIndex
    public static void mapPartitionsWithIndexEight() {
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6);
        JavaRDD<Integer> listRDD = sc.parallelize(list, 2);
        JavaRDD<String> numRDD = listRDD.mapPartitionsWithIndex((index, iterator) -> {
            ArrayList<String> array = new ArrayList<>();
            while (iterator.hasNext()) {
                array.add(index + "_" + iterator.next());
            }
            return array.iterator();
        }, true);

        numRDD.foreach(num -> System.out.println(num));
    }
}
