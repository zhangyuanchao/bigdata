package com.themis.flink.core;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;

/**
 * flink批处理wordcount
 */
public class WordCountBatch {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        // DataSet<String> text = env.readTextFile("C:\\study\\hello-flink.txt");
        DataSet<String> text = env.readTextFile("hdfs://nn-services/spark/DORRIT.txt");
        DataSet<Tuple2<String, Integer>> resultSet = text.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] words = line.split(" ");
                for (String word : words) {
                    collector.collect(new Tuple2<>(word, 1));
                }
            }
        });
        DataSet<Tuple2<String, Integer>> result = resultSet.groupBy(0).sum(1);
        // result.print();
        result.writeAsText("hdfs://nn-services/root/wc-result");
        env.execute();
    }
}
