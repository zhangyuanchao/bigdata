package com.themis.flink.window;

import com.themis.flink.model.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * 时间窗口增量聚合函数
 * Reduce和Aggregate区别
 *  1、Reduce要求输入元素类型相同，并且输出的元素类型与输入类型相同()；
 *  2、Aggregate输入元素类型可以不相同，是更一般化的聚合函数，可以认为是reduce的通用版本，可以用于求平均值等场景
 *      AggregateFunction有三个泛型类型：IN 输入类型，输入元素类型；ACC 累加器类型，中间状态类型；OUT 输出类型
 */
public class TimeWindowReduce {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        ParameterTool param = ParameterTool.fromArgs(args);
//        String host = param.get("host");
//        int port = param.getInt("port", 7777);
//        DataStream<String> inputStream = env.socketTextStream(host, port);
//        DataStream<Event> eventStream = inputStream.map(line -> {
//            String[] fields = line.split(",");
//            return new Event(fields[0], fields[1], Long.valueOf(fields[2]));
//        }).assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
//                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
//                    @Override
//                    public long extractTimestamp(Event event, long l) {
//                        return event.getTimestamp();
//                    }
//                }));
        DataStream<Event> eventStream = env.fromElements(new Event("Bob","/page",2000L),
                new Event("Alice","/page",2100L),
                new Event("Mary","/page",2200L),
                new Event("Bob","/page",2300L),
                new Event("Bob","/page",2400L),
                new Event("Alice","/page",2500L),
                new Event("Bob","/page",2600L),
                new Event("Bob","/page",2700L));
//                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(10))
//                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
//                            @Override
//                            public long extractTimestamp(Event event, long l) {
//                                return event.getTimestamp();
//                            }
//                        }));
        eventStream.map(new MapFunction<Event, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(Event event) throws Exception {
                return new Tuple2<>(event.getUser(), 1L);
            }
        })
                .keyBy(user -> user.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                        return new Tuple2<>(value1.f0, value1.f1 + value2.f1);
                    }
                }).print();
        env.execute();
    }
}
