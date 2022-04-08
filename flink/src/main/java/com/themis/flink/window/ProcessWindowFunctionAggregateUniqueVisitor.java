package com.themis.flink.window;

import com.themis.flink.model.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.HashSet;

/**
 * 使用ProcessWindowFunction结合AggregateFunction计算uv
 * https://portal.shadowsocks.nz/clientarea.php
 */
public class ProcessWindowFunctionAggregateUniqueVisitor {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(100);
        DataStream<Event> inputStream = env.addSource(new ClickSource()); // 数据源1s产生一条数据
        DataStream<Event> eventStream = inputStream.assignTimestampsAndWatermarks(WatermarkStrategy
                // watermark延迟0s
                .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event event, long l) {
                        return event.getTimestamp();
                    }
                }));
        eventStream.print("data");

        eventStream.keyBy(data -> true)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                // 使用AggregateFunction结合ProcessWindowFunction计算uv
                .aggregate(new UniqueVisitorAggregate(), new UniqueVisitorCountWindow())
                .print();

        env.execute();
    }

    // 实现自定义的AggregateFunction，增量聚合计算uv
    public static class UniqueVisitorAggregate implements AggregateFunction<Event, HashSet<String>, Integer> {

        @Override
        public HashSet<String> createAccumulator() {
            return new HashSet<>();
        }

        @Override
        public HashSet<String> add(Event event, HashSet<String> acc) {
            acc.add(event.getUser());
            return acc;
        }

        /**
         * AggregateFunction结合ProcessWindowFunction使用时，getResult的返回值作为ProcessWindowFunction实现类的输入
         * @param acc
         * @return
         */
        @Override
        public Integer getResult(HashSet<String> acc) {
            return acc.size();
        }

        @Override
        public HashSet<String> merge(HashSet<String> values, HashSet<String> acc) {
            acc.addAll(values);
            return acc;
        }
    }

    // 实现自定义的ProcessWindowFunction, 包装窗口信息输出, 结合AggregateFunction使用，输入类型就是AggregateFunction.getResult的返回类型
    public static class UniqueVisitorCountWindow extends ProcessWindowFunction<Integer, String, Boolean, TimeWindow> {
        @Override
        public void process(Boolean key, Context context, Iterable<Integer> elements, Collector<String> collector) throws Exception {

            Integer uv = elements.iterator().next();
            // 结合窗口信息
            Long start = context.window().getStart();
            Long end = context.window().getEnd();
            String output = "窗口:" + new Timestamp(start) + "~" + new Timestamp(end) + ",uv值:" + uv;
            collector.collect(output);
        }
    }
}
