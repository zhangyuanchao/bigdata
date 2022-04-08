package com.themis.flink.window;

import com.themis.flink.model.Event;
import com.themis.flink.model.UrlView;
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
 * 使用ProcessWindowFunction结合AggregateFunction统计url访问量
 */
public class ProcessWindowFunctionAggregateUrlView {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(100);
        DataStream<Event> inputStream = env.addSource(new ClickSource()); // 数据源1s产生一条数据
        DataStream<Event> eventStream = inputStream.assignTimestampsAndWatermarks(WatermarkStrategy
                // watermark延迟0s
                .<Event>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event event, long l) {
                        return event.getTimestamp();
                    }
                }));
        eventStream.print("data");

        eventStream.keyBy(data -> data.getUrl())
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                // 使用AggregateFunction结合ProcessWindowFunction统计url访问量
                .aggregate(new UrlViewCountAggregate(), new UrlViewCountWindow())
                .print();

        env.execute();
    }

    // 实现自定义的AggregateFunction，增量聚合统计url访问量
    public static class UrlViewCountAggregate implements AggregateFunction<Event, Long, Long> {


        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Event event, Long acc) {
            return ++acc;
        }

        @Override
        public Long getResult(Long acc) {
            return acc;
        }

        @Override
        public Long merge(Long acc1, Long acc2) {
            return acc1 + acc2;
        }
    }

    // 实现自定义的ProcessWindowFunction, 包装窗口信息输出, 结合AggregateFunction使用，输入类型就是AggregateFunction.getResult的返回类型
    public static class UrlViewCountWindow extends ProcessWindowFunction<Long, UrlView, String, TimeWindow> {
        @Override
        public void process(String key, Context context, Iterable<Long> elements, Collector<UrlView> collector) throws Exception {

            Long count = elements.iterator().next();
            // 结合窗口信息
            Long start = context.window().getStart();
            Long end = context.window().getEnd();
            collector.collect(new UrlView(key, count, start, end));
        }
    }
}
