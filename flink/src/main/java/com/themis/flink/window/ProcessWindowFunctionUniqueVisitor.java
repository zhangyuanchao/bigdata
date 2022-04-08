package com.themis.flink.window;

import com.themis.flink.model.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
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
 * 使用ProcessWindowFunction计算uv
 */
public class ProcessWindowFunctionUniqueVisitor {

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
                // 使用自定义的ProcessWindowFunction计算uv
                .process(new UniqueVisitorCountWindow())
                .print();

        env.execute();
    }

    // 实现自定义的ProcessWindowFunction, 输出一条uv统计信息
    public static class UniqueVisitorCountWindow extends ProcessWindowFunction<Event, String, Boolean, TimeWindow> {
        @Override
        public void process(Boolean key, Context context, Iterable<Event> elements, Collector<String> collector) throws Exception {
            // 保存user，并去重
            HashSet<String> users = new HashSet<>();
            for (Event element : elements) {
                users.add(element.getUser());
            }
            Integer uv = users.size();
            // 结合窗口信息
            Long start = context.window().getStart();
            Long end = context.window().getEnd();
            String output = "窗口:" + new Timestamp(start) + "~" + new Timestamp(end) + ",uv值:" + uv;
            collector.collect(output);
        }
    }
}
