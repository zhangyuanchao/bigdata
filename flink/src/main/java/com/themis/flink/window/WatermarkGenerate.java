package com.themis.flink.window;

import com.themis.flink.model.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/**
 * 使用flink内置的时间戳提取器和Watermark生成器
 */
public class WatermarkGenerate {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setAutoWatermarkInterval(100);
        DataStream<Event> inputStream = env.fromElements(
                new Event("Bob", "/home", 3000L),
                new Event("Jame", "/prod?id=1", 3100L),
                new Event("Lucy", "/prod?id=2", 3200L)
        );

        /**
         * forMonotonousTimestamps:处理有序数据流(数据时间戳递增)
         * forBoundedOutOfOrderness:处理有边界无序数据流的Watermark
         * SerializableTimestampAssigner继承TimestampAssigner
         * TimestampAssigner:时间分配器(时间提取器)，提取当前数据里的时间字段，给数据打上时间戳，并基于时间戳生成Watermark
         */
        // 有序流的Watermark生成, 使用forMonotonousTimestamps()方法
        inputStream.assignTimestampsAndWatermarks(WatermarkStrategy
                .<Event>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event element, long recordTimestamp) {
                        return element.getTimestamp();
                    }
                }));
        // 乱序流的Watermark生成, 使用<T> WatermarkStrategy<T> forBoundedOutOfOrderness(Duration maxOutOfOrderness)
        // 参数表示最大乱序程度, 也是延迟时间
        inputStream.assignTimestampsAndWatermarks(WatermarkStrategy
                .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event element, long recordTimestamp) {
                        return 0;
                    }
                }));
    }
}
