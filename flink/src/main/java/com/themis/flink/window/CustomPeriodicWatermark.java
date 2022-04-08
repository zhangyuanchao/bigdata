package com.themis.flink.window;

import com.themis.flink.model.Event;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/**
 * 自定义周期性地产生Watermark
 */
public class CustomPeriodicWatermark {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setAutoWatermarkInterval(100);
        DataStream<Event> inputStream = env.fromElements(
                new Event("Bob", "/home", 3000L),
                new Event("Jame", "/prod?id=1", 3100L),
                new Event("Lucy", "/prod?id=2", 3200L)
        );
    }

    public static class PeriodicWatermarkStrategy implements WatermarkStrategy<Event> {

        @Override
        public WatermarkGenerator<Event> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
            return new CustomBoundedOutOfOrdernessGenerator();
        }

        @Override
        public TimestampAssigner<Event> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
            return new SerializableTimestampAssigner<Event>() {
                @Override
                public long extractTimestamp(Event element, long recordTimestamp) {
                    return element.getTimestamp(); // 告诉程序数据源里的时间戳字段是哪个
                }
            };
        }
    }

    /**
     * 周期性地生成Watermark
     */
    public static class CustomBoundedOutOfOrdernessGenerator implements WatermarkGenerator<Event> {

        private Long delayTime = 5000L; // 延迟时间
        private Long maxTs = -Long.MAX_VALUE + delayTime + 1L; // 观察到的最大时间戳

        @Override
        public void onEvent(Event event, long eventTimestamp, WatermarkOutput output) {
            // 每来一条数据就调用一次
            maxTs = Math.max(event.getTimestamp(), maxTs); // 更新最大时间戳
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            // 发射水位线, 默认200ms调用一次
            output.emitWatermark(new Watermark(maxTs - delayTime - 1L));
        }
    }
}
