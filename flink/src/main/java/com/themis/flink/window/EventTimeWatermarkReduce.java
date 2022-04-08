package com.themis.flink.window;

import com.themis.flink.model.Event;
import com.themis.flink.model.SensorReading;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * 时间窗口增量聚合函数
 */
public class EventTimeWatermarkReduce {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        ParameterTool parameter = ParameterTool.fromArgs(args);
//        String host = parameter.get("host");
//        Integer port = parameter.getInt("port", 7777);
//        DataStream<String> inputStream = env.socketTextStream(host, port);
//        DataStream<Event> dataStream = inputStream.map(new MapFunction<String, Event>() {
//            @Override
//            public Event map(String line) throws Exception {
//                String[] fields = line.split(",");
//                return new Event(fields[0], fields[1], new Long(fields[2]));
//            }
//        });
        DataStream<Event> dataStream = env.fromElements(
                new Event("Bob", "/home", 3000L),
                new Event("Jame", "/prod?id=1", 3100L),
                new Event("Lucy", "/prod?id=2", 3200L),
                new Event("Bob", "/home", 3300L),
                new Event("Jame", "/prod?id=1", 3400L),
                new Event("Bob", "/home", 3500L),
                new Event("Jame", "/prod?id=1", 3600L)
        );

//        // 开窗测试
        DataStream<Tuple2<String, Long>> outputStream = dataStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5)).withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event event, long l) {
                        return event.getTimestamp();
                    }
                }))
                .map(new MapFunction<Event, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(Event event) throws Exception {
                        return new Tuple2<>(event.getUser(), 1L);
                    }
                })
                .keyBy(new KeySelector<Tuple2<String, Long>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Long> value) throws Exception {
                        return value.f0;
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.seconds(15)))
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                        return new Tuple2<>(value1.f0, value1.f1 + value2.f1);
                    }
                });
        outputStream.print();
        env.execute();
    }
}
