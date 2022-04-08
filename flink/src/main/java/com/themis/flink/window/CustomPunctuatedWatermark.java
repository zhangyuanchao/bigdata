package com.themis.flink.window;

import com.themis.flink.model.Event;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 自定义断点式地产生Watermark
 */
public class CustomPunctuatedWatermark {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setAutoWatermarkInterval(100);
        DataStream<Event> inputStream = env.fromElements(
                new Event("Bob", "/home", 3000L),
                new Event("Jame", "/prod?id=1", 3100L),
                new Event("Lucy", "/prod?id=2", 3200L)
        );
    }

    public static class PunctuatedWatermarkStrategy implements WatermarkStrategy<Event> {

        @Override
        public WatermarkGenerator<Event> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
            return new CustomPunctuatedGenerator();
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

    public static class CustomPunctuatedGenerator implements WatermarkGenerator<Event> {

        private Long delayTime = 5000L; // 延迟时间
        private Long maxTs = -Long.MAX_VALUE + delayTime + 1L; // 观察到的最大时间戳

        @Override
        public void onEvent(Event event, long eventTimestamp, WatermarkOutput output) {
            // 只有在遇到特定地user时才发出水位线
            if (event.getUser().equals("Mary")) {
                output.emitWatermark(new Watermark(event.getTimestamp() - 1L));
            }
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            // 不需要做任何事，在onEvent中已经发射了水位线
        }
    }
}
