package com.themis.flink.transformation;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class TransformationBase {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> inputStream = env.readTextFile("C:\\study\\workspace\\java\\bd\\bigdata\\flink\\src\\main\\resources\\sensor.txt");
        DataStream<Integer> mapStream = inputStream.map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String value) throws Exception {
                return value.length();
            }
        });

        DataStream<String> flatMapStream = inputStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String line, Collector<String> collector) throws Exception {
                String[] values = line.split(",");
                for (String value : values) {
                    collector.collect(value);
                }
            }
        });

        DataStream<String> filterStream = inputStream.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                return value.startsWith("sensor_1");
            }
        });

        mapStream.print("map");
        flatMapStream.print("flatMap");
        filterStream.print("filter");

        env.execute();
    }
}
