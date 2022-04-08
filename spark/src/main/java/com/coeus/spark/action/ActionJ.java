package com.coeus.spark.action;

import org.apache.spark.HashPartitioner;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class ActionJ {

    static SparkConf conf = null;
    static JavaSparkContext sc = null;

    static {
        conf = new SparkConf().setMaster("local").setAppName("TransformationJ");
        sc = new JavaSparkContext(conf);
    }

    public static void main(String[] args) {
        // reduceSeven();
        // reduceEight();

        // reduceByKeySeven();
        // reduceByKeyEight();

        // unionSeven();
        // unionEight();

        // groupByKeySeven();
        // groupByKeyEight();

        // joinSeven();
        // joinEight();

        // sampleSeven();
        // sampleEight();

        // cartesianSeven();
        // cartesianEight();

        // filterSeven();
        // filterEight();

        // distinctSeven();
        // distinctEight();

        // intersectionSeven();
        // intersectionEight();

        // repartitionAndSortWithinPartitionsSeven();
        // repartitionAndSortWithinPartitionsEight();

        // cogroupSeven();

        // sortByKeySeven();

        // aggregateSeven();
        aggregateByKeySeven();
    }

    // java7编写reduce
    public static void reduceSeven() {
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6);
        JavaRDD<Integer> listRDD = sc.parallelize(list, 3);
        Integer result = listRDD.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer i1, Integer i2) throws Exception {
                return i1 + i2;
            }
        });
        System.out.println(result);
    }

    // java7编写reduceByKey
    public static void reduceByKeySeven() {
        List<Tuple2<String, Integer>> list = Arrays.asList(
                new Tuple2<String, Integer>("武当", 99),
                new Tuple2<String, Integer>("少林", 97),
                new Tuple2<String, Integer>("武当", 89),
                new Tuple2<String, Integer>("少林", 77));
        JavaPairRDD<String, Integer> listRDD = sc.parallelizePairs(list);
        // 运行reduceByKey方法时，会将key值相同的组合在一起做call方法中的操作
        JavaPairRDD<String, Integer> pairRDD = listRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer i1, Integer i2) throws Exception {
                return i1 + i2;
            }
        });
        pairRDD.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> t) throws Exception {
                System.out.println("门派:" + t._1 + ", 人数:" + t._2);
            }
        });
    }

    // java7编写union
    public static void unionSeven() {
        List<Integer> list1 = Arrays.asList(1, 2, 3, 4);
        List<Integer> list2 = Arrays.asList(3, 4, 5, 6);
        JavaRDD<Integer> list1RDD = sc.parallelize(list1);
        JavaRDD<Integer> list2RDD = sc.parallelize(list2);
        list1RDD.union(list2RDD).foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer num) throws Exception {
                System.out.println("number:" + num);
            }
        });
    }

    // java7编写groupByKey
    public static void groupByKeySeven() {
        List<Tuple2<String,String>> list = Arrays.asList(
                new Tuple2("武当", "张三丰"),
                new Tuple2("峨眉", "灭绝师太"),
                new Tuple2("武当", "宋青书"),
                new Tuple2("峨眉", "周芷若")
        );
        JavaPairRDD<String, String> listRDD = sc.parallelizePairs(list);
        JavaPairRDD<String, Iterable<String>> groupRDD = listRDD.groupByKey();
        groupRDD.foreach(new VoidFunction<Tuple2<String, Iterable<String>>>() {
            @Override
            public void call(Tuple2<String, Iterable<String>> tuple) throws Exception {
                String menpai = tuple._1;
                Iterator<String> iter = tuple._2.iterator();
                String people = "";
                while (iter.hasNext()) {
                    people = people + iter.next() + " ";
                }
                System.out.println("门派:" + menpai + "-> 人员:" + people);
            }
        });
    }

    // java7编写join
    public static void joinSeven() {
        List<Tuple2<Integer, String>> names = Arrays.asList(
                new Tuple2<Integer, String>(1, "东方不败"),
                new Tuple2<Integer, String>(2, "令狐冲"),
                new Tuple2<Integer, String>(3, "林平之")
        );
        List<Tuple2<Integer, Integer>> scores = Arrays.asList(
                new Tuple2<Integer, Integer>(1, 99),
                new Tuple2<Integer, Integer>(2, 98),
                new Tuple2<Integer, Integer>(3, 97)
        );
        JavaPairRDD<Integer, String> nameRDD = sc.parallelizePairs(names);
        JavaPairRDD<Integer, Integer> scoreRDD = sc.parallelizePairs(scores);
        JavaPairRDD<Integer, Tuple2<String, Integer>> joinRDD = nameRDD.join(scoreRDD);
        // JavaPairRDD<Integer, Tuple2<Integer, String>> joinRDD = scoreRDD.join(nameRDD);
        joinRDD.foreach(new VoidFunction<Tuple2<Integer, Tuple2<String, Integer>>>() {
            @Override
            public void call(Tuple2<Integer, Tuple2<String, Integer>> tuple) throws Exception {
                System.out.println("学号:" + tuple._1 + ", 姓名:" + tuple._2._1 + ", 成绩:" + tuple._2._2);
            }
        });
    }

    // java7编写simple
    public static void sampleSeven() {
        ArrayList<Integer> list = new ArrayList<>();
        for (int i = 1; i <= 100; i++) {
            list.add(i);
        }
        JavaRDD<Integer> listRDD = sc.parallelize(list);
        /**
         * sample用来从RDD中抽取样本。他有三个参数
         * withReplacement: Boolean,
         *      true: 有放回的抽样
         *      false: 无放回抽象
         * fraction: Double：
         *      抽取样本的比例
         * seed: Long：
         *      随机种子
         */
        JavaRDD<Integer> sampleRDD = listRDD.sample(false, 0.1, 1);
        sampleRDD.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer i) throws Exception {
                System.out.print(i + " ");
            }
        });
    }

    // java7编写cartesian
    public static void cartesianSeven() {
        List<String> list1 = Arrays.asList("A", "B");
        List<Integer> list2 = Arrays.asList(1, 2, 3);
        JavaRDD<String> list1RDD = sc.parallelize(list1);
        JavaRDD<Integer> list2RDD = sc.parallelize(list2);
        JavaPairRDD<String, Integer> cartesianRDD = list1RDD.cartesian(list2RDD);
        cartesianRDD.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> tuple) throws Exception {
                System.out.println(tuple._1 + "->" + tuple._2);
            }
        });
    }

    // java7编写filter
    public static void filterSeven() {
        List<Integer> list = Arrays.asList(1, 5, 6, 9 ,2, 5, 4, 8, 10 ,1002);
        JavaRDD<Integer> listRDD = sc.parallelize(list);
        JavaRDD<Integer> filterRDD = listRDD.filter(new Function<Integer, Boolean>() {
            @Override
            public Boolean call(Integer num) throws Exception {
                return num % 2 == 0;
            }
        });
        filterRDD.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer num) throws Exception {
                System.out.println(num);
            }
        });
    }

    // java7编写distinct
    public static void distinctSeven() {
        List<Integer> list = Arrays.asList(1, 1, 2, 2, 3, 3, 4, 5);
        JavaRDD<Integer> listRDD = sc.parallelize(list);
        JavaRDD<Integer> distinctRDD = listRDD.distinct();
        distinctRDD.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer num) throws Exception {
                System.out.println(num);
            }
        });
    }

    // java7编写intersection
    public static void intersectionSeven() {
        List<Integer> list1 = Arrays.asList(1, 2, 3, 4);
        List<Integer> list2 = Arrays.asList(3, 4, 5, 6);
        JavaRDD<Integer> list1RDD = sc.parallelize(list1);
        JavaRDD<Integer> list2RDD = sc.parallelize(list2);
        list1RDD.intersection(list2RDD).foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer num) throws Exception {
                System.out.println(num);
            }
        });
    }

    // java7编写repartitionAndSortWithinPartitions

    /**
     * repartitionAndSortWithinPartitions对RDD分区，并且在每个分区内排序
     */
    public static void repartitionAndSortWithinPartitionsSeven() {
        List<Integer> list = Arrays.asList(1, 3, 55, 77, 33, 5, 23, 4, 66);
        JavaRDD<Integer> listRDD = sc.parallelize(list);
        JavaPairRDD<Integer, Integer> pairRDD = listRDD.mapToPair(new PairFunction<Integer, Integer, Integer>() {
            @Override
            public Tuple2<Integer, Integer> call(Integer num) throws Exception {
                return new Tuple2<>(num, num);
            }
        });
        JavaPairRDD<Integer, Integer> partitionRDD = pairRDD.repartitionAndSortWithinPartitions(new Partitioner() {
            @Override
            public int numPartitions() {
                return 2;
            }

            @Override
            public int getPartition(Object key) {
                Integer index = Integer.valueOf(key.toString());
                if (index % 2 == 0) {
                    return 0;
                } else {
                    return 1;
                }
            }
        });

        JavaRDD<String> mapPartitionsRDD = partitionRDD.mapPartitionsWithIndex(new Function2<Integer, Iterator<Tuple2<Integer, Integer>>, Iterator<String>>() {
            @Override
            public Iterator<String> call(Integer index, Iterator<Tuple2<Integer, Integer>> iterator) throws Exception {
                ArrayList<String> l = new ArrayList<>();
                while (iterator.hasNext()) {
                    l.add(index + "_" + iterator.next());
                }
                return l.iterator();
            }
        }, false);

        mapPartitionsRDD.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });
    }

    // java7编写cogroup

    /**
     * 对KV元素的RDD，将两个RDD中key相同的value合并为一个集合
     */
    public static void cogroupSeven() {
        List<Tuple2<Integer, String>> list1 = Arrays.asList(
                new Tuple2<Integer, String>(1, "www"),
                new Tuple2<Integer, String>(2, "bbs")
        );

        List<Tuple2<Integer, String>> list2 = Arrays.asList(
                new Tuple2<Integer, String>(1, "cnblog"),
                new Tuple2<Integer, String>(2, "cnblog"),
                new Tuple2<Integer, String>(3, "very")
        );

        List<Tuple2<Integer, String>> list3 = Arrays.asList(
                new Tuple2<Integer, String>(1, "com"),
                new Tuple2<Integer, String>(2, "com"),
                new Tuple2<Integer, String>(3, "good")
        );
        JavaPairRDD<Integer, String> list1RDD = sc.parallelizePairs(list1);
        JavaPairRDD<Integer, String> list2RDD = sc.parallelizePairs(list2);
        JavaPairRDD<Integer, String> list3RDD = sc.parallelizePairs(list3);
        JavaPairRDD<Integer, Tuple3<Iterable<String>, Iterable<String>, Iterable<String>>> cogroupRDD = list1RDD.cogroup(list2RDD, list3RDD);
        cogroupRDD.foreach(new VoidFunction<Tuple2<Integer, Tuple3<Iterable<String>, Iterable<String>, Iterable<String>>>>() {
            @Override
            public void call(Tuple2<Integer, Tuple3<Iterable<String>, Iterable<String>, Iterable<String>>> tuple) throws Exception {
                System.out.println("key:" + tuple._1 + ",v1:" + tuple._2._1() + ",v2:" + tuple._2._2() + ",v3:" + tuple._2._3());
                Iterable<String> i1 = tuple._2._1();


            }
        });
    }

    // java7编写sortByKey
    public static void sortByKeySeven() {
        List<Tuple2<Integer, String>> list = Arrays.asList(
                new Tuple2<>(99, "张三丰"),
                new Tuple2<>(96, "东方不败"),
                new Tuple2<>(66, "林平之"),
                new Tuple2<>(98, "聂风")
        );
        JavaPairRDD<Integer, String> listRDD = sc.parallelizePairs(list, 2);
        listRDD.sortByKey().foreach(new VoidFunction<Tuple2<Integer, String>>() {
            @Override
            public void call(Tuple2<Integer, String> tuple) throws Exception {
                System.out.println("等级:" + tuple._1 + ", 人物" + tuple._2);
            }
        });
    }

    // java7编写aggregate
    public static void aggregateSeven() {
        List<Integer> list = Arrays.asList(3, 8, 7, 9, 4, 1, 2);
        // 0, 2
        // 2, 3

        JavaRDD<Integer> listRDD = sc.parallelize(list, 3);
        Integer result = listRDD.aggregate(0, new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer i1, Integer i2) throws Exception {
                return i2-i1;
            }
        }, new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer i1, Integer i2) throws Exception {
                return i1 + i2;
            }
        });
        System.out.println(result); // 12
    }

    // java7编写aggregateByKey
    public static void aggregateByKeySeven(){
        List<String> list = Arrays.asList("you,jump", "i,jump", "he,jump");
        /**
         * parallelize分区规则:
         *  第一个参数表示需要分区的列表list, list大小为len, 例如:list = [7, 3, 9, 4, 5], list大小为5
         *  第二个参数表示要分区的个数n, 例如n = 3
         *  分区规则:
         *      下标在[0, len/n)的元素分到0号分区, 即元素7在第一个分区
         *      下标在[len/n, 2*len/n)的元素分到1号分区, 即元素3, 9在第二个分区
         *      下标在[2*len/n, 3*len/n)的元素分到2号分区, 即元素4, 5在第三个分区
         *      .
         *      .
         *      .
         *
         */
        JavaRDD<String> listRDD = sc.parallelize(list, 2);
        JavaRDD<String> flatMapRDD = listRDD.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(",")).iterator();
            }
        });
        JavaPairRDD<String, Integer> pairRDD = flatMapRDD.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });

        /**
         * aggregateByKey存在函数颗粒化，有两个参数列表
         * 第一个参数列表，需要传递一个参数，表示为初始值
         * 主要当碰见第一个key时候，和value进行分区内计算
         * 第二个参数列表，需要传递2个参数
         * 第一个参数表示分区内计算
         * 第二个参数表示分区间计算, 分区间有相同的key才会执行第二个参数的方法，如果某一个key为一个分区所独有，则不会执行第二个参数的方法
         */
        JavaPairRDD<String, Integer> aggregateRDD = pairRDD.aggregateByKey(2, new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer i1, Integer i2) throws Exception {
                return i1 + i2;
            }
        }, new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer i1, Integer i2) throws Exception {
                return (i1*4 + i2*2) * 3;
            }
        });
        aggregateRDD.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> tuple) throws Exception {
                System.out.println(tuple._1+"->"+tuple._2);
            }
        });
//        listRDD.flatMap(new FlatMapFunction<String, String>() {
//            @Override
//            public Iterator<String> call(String line) throws Exception {
//                return Arrays.asList(line.split(",")).iterator();
//            }
//        }).mapToPair(new PairFunction<String, String, Integer>() {
//            @Override
//            public Tuple2<String, Integer> call(String word) throws Exception {
//                return new Tuple2<>(word,1);
//            }
//        }).aggregateByKey(0, new Function2<Integer, Integer, Integer>() {
//            @Override
//            public Integer call(Integer i1, Integer i2) throws Exception {
//                return i1 + i2;
//            }
//        }, new Function2<Integer, Integer, Integer>() {
//            @Override
//            public Integer call(Integer i1, Integer i2) throws Exception {
//                return i1+i2;
//            }
//        }).foreach(new VoidFunction<Tuple2<String, Integer>>() {
//            @Override
//            public void call(Tuple2<String, Integer> tuple) throws Exception {
//                System.out.println(tuple._1+"->"+tuple._2);
//            }
//        });
    }

    /***********************************************************/
    // java8编写reduce
    public static void reduceEight() {
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6);
        JavaRDD<Integer> listRDD = sc.parallelize(list);
        Integer result = listRDD.reduce((i1, i2) -> i1 + i2);
        System.out.println(result);
    }

    // java8编写reduceByKey
    public static void reduceByKeyEight() {
        List<Tuple2<String, Integer>> list = Arrays.asList(
                new Tuple2<String, Integer>("武当", 99),
                new Tuple2<String, Integer>("少林", 97),
                new Tuple2<String, Integer>("武当", 89),
                new Tuple2<String, Integer>("少林", 77)
        );
        JavaPairRDD<String, Integer> listRDD = sc.parallelizePairs(list);
        JavaPairRDD<String, Integer> pairRDD = listRDD.reduceByKey((x, y) -> x + y);
        pairRDD.foreach(t -> System.out.println("门派:" + t._1 + ", 人数:" + t._2));
    }

    // java8编写union
    public static void unionEight() {
        List<Integer> list1 = Arrays.asList(1, 2, 3, 4);
        List<Integer> list2 = Arrays.asList(3, 4, 5, 6);
        JavaRDD<Integer> list1RDD = sc.parallelize(list1);
        JavaRDD<Integer> list2RDD = sc.parallelize(list2);
        list1RDD.union(list2RDD).foreach(num -> System.out.println(num));
    }

    // java8编写groupByKey
    public static void groupByKeyEight() {
        List<Tuple2<String,String>> list = Arrays.asList(
                new Tuple2("武当", "张三丰"),
                new Tuple2("峨眉", "灭绝师太"),
                new Tuple2("武当", "宋青书"),
                new Tuple2("峨眉", "周芷若")
        );
        JavaPairRDD<String, String> listRDD = sc.parallelizePairs(list);
        JavaPairRDD<String, Iterable<String>> groupRDD = listRDD.groupByKey();
        groupRDD.foreach(tuple -> {
            String menpai = tuple._1;
            Iterator<String> iter = tuple._2.iterator();
            String people = "";
            while (iter.hasNext()) {
                people = people + iter.next() + " ";
            }
            System.out.println("门派:" + menpai + "->人员:" + people);
        });
    }

    // java8编写join
    public static void joinEight() {
        List<Tuple2<Integer, String>> names = Arrays.asList(
                new Tuple2<Integer, String>(1, "东方不败"),
                new Tuple2<Integer, String>(2, "令狐冲"),
                new Tuple2<Integer, String>(3, "林平之")
        );
        List<Tuple2<Integer, Integer>> scores = Arrays.asList(
                new Tuple2<Integer, Integer>(1, 99),
                new Tuple2<Integer, Integer>(2, 98),
                new Tuple2<Integer, Integer>(3, 97)
        );
        List<Tuple2<Integer, String>> cities = Arrays.asList(
                new Tuple2<Integer, String>(1, "北京"),
                new Tuple2<Integer, String>(2, "香港"),
                new Tuple2<Integer, String>(3, "伦敦")
        );
        JavaPairRDD<Integer, String> nameRDD = sc.parallelizePairs(names);
        JavaPairRDD<Integer, Integer> scoreRDD = sc.parallelizePairs(scores);
        JavaPairRDD<Integer, String> cityRDD = sc.parallelizePairs(cities);
        JavaPairRDD<Integer, Tuple2<String, Integer>> joinRDD = nameRDD.join(scoreRDD);
        joinRDD.foreach(t -> System.out.println(t._1 + "," + t._2._1 + "," + t._2._2));
    }

    // java8编写sample
    public static void sampleEight() {
        ArrayList<Integer> list = new ArrayList<>();
        for (int i = 1; i <= 100; i++) {
            list.add(i);
        }
        JavaRDD<Integer> listRDD = sc.parallelize(list);
        JavaRDD<Integer> sampleRDD = listRDD.sample(false, 0.1, 1);
        sampleRDD.foreach(num -> System.out.println(num + " "));
    }

    // java8编写cartesian
    public static void cartesianEight() {
        List<String> list1 = Arrays.asList("A", "B");
        List<Integer> list2 = Arrays.asList(1, 2, 3);
        JavaRDD<String> list1RDD = sc.parallelize(list1);
        JavaRDD<Integer> list2RDD = sc.parallelize(list2);
        JavaPairRDD<String, Integer> cartesianRDD = list1RDD.cartesian(list2RDD);
        cartesianRDD.foreach(t -> System.out.println(t._1 + "->" + t._2));
    }

    // java8编写filter
    public static void filterEight() {
        List<Integer> list = Arrays.asList(3, 8, 2, 9, 1, 12, 1004, 6, 7, 35);
        JavaRDD<Integer> listRDD = sc.parallelize(list);
        JavaRDD<Integer> filterRDD = listRDD.filter(x -> x % 2 == 0);
        filterRDD.foreach(x -> System.out.println(x));
    }

    // java8编写distinct
    public static void distinctEight() {
        List<Integer> list = Arrays.asList(1, 1, 2, 2, 3, 3, 4, 5);
        JavaRDD<Integer> listRDD = sc.parallelize(list);
        JavaRDD<Integer> distinctRDD = listRDD.distinct();
        distinctRDD.foreach(x -> System.out.println(x));
    }

    // java8编写intersection
    public static void intersectionEight() {
        List<Integer> list1 = Arrays.asList(1, 2, 3, 4);
        List<Integer> list2 = Arrays.asList(3, 4, 5, 6);
        JavaRDD<Integer> list1RDD = sc.parallelize(list1);
        JavaRDD<Integer> list2RDD = sc.parallelize(list2);
        list1RDD.intersection(list2RDD).foreach(x -> System.out.println(x));
    }

    // java8编写repartitionAndSortWithinPartitions
    public static void repartitionAndSortWithinPartitionsEight() {
        List<Integer> list = Arrays.asList(1, 4, 55, 66, 33, 48, 23);
        JavaRDD<Integer> listRDD = sc.parallelize(list);
        JavaPairRDD<Integer, Integer> pairRDD = listRDD.mapToPair(num -> new Tuple2(num, num));
        pairRDD.repartitionAndSortWithinPartitions(new HashPartitioner(2)).mapPartitionsWithIndex((index, iterator) -> {
            ArrayList<String> l = new ArrayList<>();
            while (iterator.hasNext()) {
                l.add(index + "_" + iterator.next());
            }
            return l.iterator();
        }, false).foreach(s -> System.out.println(s));
    }

    // java8编写cogroup
    public static void cogroupEight() {
        List<Tuple2<Integer, String>> list1 = Arrays.asList(
                new Tuple2<Integer, String>(1, "www"),
                new Tuple2<Integer, String>(2, "bbs")
        );

        List<Tuple2<Integer, String>> list2 = Arrays.asList(
                new Tuple2<Integer, String>(1, "cnblog"),
                new Tuple2<Integer, String>(2, "cnblog"),
                new Tuple2<Integer, String>(3, "very")
        );

        List<Tuple2<Integer, String>> list3 = Arrays.asList(
                new Tuple2<Integer, String>(1, "com"),
                new Tuple2<Integer, String>(2, "com"),
                new Tuple2<Integer, String>(3, "good")
        );
        JavaPairRDD<Integer, String> list1RDD = sc.parallelizePairs(list1);
        JavaPairRDD<Integer, String> list2RDD = sc.parallelizePairs(list2);
        JavaPairRDD<Integer, String> list3RDD = sc.parallelizePairs(list3);
        JavaPairRDD<Integer, Tuple3<Iterable<String>, Iterable<String>, Iterable<String>>> cogroupRDD = list1RDD.cogroup(list2RDD, list3RDD);
        cogroupRDD.foreach(tuple -> {
            System.out.println("key:" + tuple._1 + ",v1:" + tuple._2._1() + ",v2:" + tuple._2._2() + ",v3:" + tuple._2._3());
            Iterable<String> i1 = tuple._2._1();
        });
    }

    // java8编写sortByKey
    public static void sortByKeyEight(){
        List<Tuple2<Integer, String>> list = Arrays.asList(
                new Tuple2<>(99, "张三丰"),
                new Tuple2<>(96, "东方不败"),
                new Tuple2<>(66, "林平之"),
                new Tuple2<>(98, "聂风")
        );
        JavaPairRDD<Integer, String> listRDD = sc.parallelizePairs(list);
        listRDD.sortByKey(false).foreach(tuple ->System.out.println(tuple._2+"->"+tuple._1));
    }

    // java8编写aggregate
    public static void aggregateEight() {
        List<Integer> list = Arrays.asList(3, 8, 7, 9, 4, 1, 2);
        // 0, 2
        // 2, 3

        JavaRDD<Integer> listRDD = sc.parallelize(list, 3);
        Integer result = listRDD.aggregate(0, (x, y) -> y -x, (m, n) -> m + n);
        System.out.println(result); // 12
    }

    // java8编写aggregateByKey
    public static void aggregateByKeyEight() {
        List<String> list = Arrays.asList("you,jump", "i,jump");
        JavaRDD<String> listRDD = sc.parallelize(list);
        listRDD.flatMap(line -> Arrays.asList(line.split(",")).iterator())
                .mapToPair(word -> new Tuple2<>(word,1))
                .aggregateByKey(0,(x,y)-> x+y,(m,n) -> m+n)
                .foreach(tuple -> System.out.println(tuple._1+"->"+tuple._2));
    }
}
