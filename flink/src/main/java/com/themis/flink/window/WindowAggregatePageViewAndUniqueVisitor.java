package com.themis.flink.window;

import com.themis.flink.model.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.HashSet;

/**
 * 统计pv和uv
 *  pv：page view
 *  uv: unique visitor
 *  pv/uv: 得到平均用户活跃度 Diagram for studying framework and component
 */
public class WindowAggregatePageViewAndUniqueVisitor {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(100);
        DataStream<Event> inputStream = env.addSource(new ClickSource()); // 数据源1s产生一条数据
        DataStream<Event> eventStream = inputStream.assignTimestampsAndWatermarks(WatermarkStrategy
                // watermark延迟5s
                .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event event, long l) {
                        return event.getTimestamp();
                    }
                }));
        eventStream.print("data");
        eventStream.keyBy(user -> true) // 所有数据分成一组
                // .window(TumblingEventTimeWindows.of(Time.seconds(10))) // 滚动窗口
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(2))) // 滑动窗口，2s滑动一次
                .aggregate(new AvgPvAggregate())
                .print();

        env.execute();
    }

    /**
     * 对于ACC类型(uple2<Long, HashSet<String>>)
     *  Long类型保存页面点击次数
     *  HashSet<String> 保存uv
     */
    public static class AvgPvAggregate implements AggregateFunction<Event, Tuple2<Long, HashSet<String>>, Double> {

        @Override
        public Tuple2<Long, HashSet<String>> createAccumulator() {
            return new Tuple2<>(0L, new HashSet<>());
        }

        @Override
        public Tuple2<Long, HashSet<String>> add(Event event, Tuple2<Long, HashSet<String>> acc) {
            // 每来一条数据调用一次
            acc.f1.add(event.getUser());
            return Tuple2.of(acc.f0 + 1, acc.f1);
        }

        @Override
        public Double getResult(Tuple2<Long, HashSet<String>> acc) {
            return (double)acc.f0 / acc.f1.size();
        }

        @Override
        public Tuple2<Long, HashSet<String>> merge(Tuple2<Long, HashSet<String>> value, Tuple2<Long, HashSet<String>> acc) {
            acc.f1.addAll(value.f1);
            return Tuple2.of(acc.f0 + value.f0, acc.f1);
        }
    }
}
