package com.themis.flink.window;


import com.themis.flink.model.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * 统计某个时间区间内每个用户点击次数
 */
public class EventTimeWindowReduce {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Event> inputStream = env.fromElements(
                new Event("Bob", "/home", 3000L),
                new Event("Jame", "/prod?id=1", 3100L),
                new Event("Lucy", "/prod?id=2", 3200L),
                new Event("Bob", "/home", 3300L),
                new Event("Jame", "/prod?id=1", 3400L),
                new Event("Bob", "/home", 3500L),
                new Event("Jame", "/prod?id=1", 3600L)
        );

        DataStream<Tuple2<String, Long>> outputStream = inputStream.map(new MapFunction<Event, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(Event event) throws Exception {
                return new Tuple2<>(event.getUser(), 1L);
            }
        })
                .keyBy(value -> value.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
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
