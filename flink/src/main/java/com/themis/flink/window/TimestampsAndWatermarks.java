package com.themis.flink.window;

import com.themis.flink.model.Event;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 提取时间戳和生成Watermark
 */
public class TimestampsAndWatermarks {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setAutoWatermarkInterval(100);
        DataStream<Event> inputStream = env.fromElements(
                new Event("Bob", "/home", 3000L),
                new Event("Jame", "/prod?id=1", 3100L),
                new Event("Lucy", "/prod?id=2", 3200L)
        );
        /**
         * TimestampAssigner主要负责从流中数据元素的某个字段中提取时间戳, 并分配给元素. 时间戳的分配是生成水位线的基础.
         * WatermarkGenerator主要负责按照既定的方式, 基于时间戳生成水位线。在WatermarkGenerator主要有两个方法:onEvent()和onPeriodicEmit()
         *      对应两种生成Watermark的方式:基于事件断点式生成和周期生成
         *      1、onEvent():每个事件(数据)到来都会调用的方法, 它的参数有当前事件、时间戳以及允许发出水位线的一个WatermarkOutput, 可以基于事件做各种操作.
         *      2、onPeriodicEmit():周期性调用的方法, 可以由WatermarkOutput发出水位线. 周期时间为处理时间,
         *          可以调用环境配置的setAutoWatermarkInterval(), 方法来设置, 默认200ms. env.getConfig().setAutoWatermarkInterval(100);
         *
         */
        inputStream.assignTimestampsAndWatermarks(new WatermarkStrategy<Event>() {

            /**
             * 提起时间戳的分配器, 提取数据里的时间戳
             * @param context
             * @return
             */
            @Override
            public TimestampAssigner<Event> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
                return WatermarkStrategy.super.createTimestampAssigner(context);
            }

            /**
             * Watermark生成器, 基于createTimestampAssigner方法提取的时间戳生成Watermark, 然后把Watermark发射出去
             * @param context
             * @return
             */
            @Override
            public WatermarkGenerator<Event> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                return null;
            }
        });
    }
}
