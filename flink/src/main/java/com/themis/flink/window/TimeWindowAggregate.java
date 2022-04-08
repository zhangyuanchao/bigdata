package com.themis.flink.window;

import com.themis.flink.model.SensorReading;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * 时间窗口增量聚合函数
 */
public class TimeWindowAggregate {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool parameter = ParameterTool.fromArgs(args);
        String host = parameter.get("host");
        Integer port = parameter.getInt("port", 7777);
        DataStream<String> inputStream = env.socketTextStream(host, port);
        DataStream<SensorReading> dataStream = inputStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String line) throws Exception {
                String[] fields = line.split(",");
                return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
            }
        });

//        // 开窗测试
        DataStream<Integer> outputStream = dataStream
                .keyBy(new KeySelector<SensorReading, String>() {
                    @Override
                    public String getKey(SensorReading sensorReading) throws Exception {
                        return sensorReading.getId();
                    }
                })
                .window(TumblingProcessingTimeWindows.of(Time.seconds(15)))
                .aggregate(new AggregateFunction<SensorReading, Integer, Integer>() {
                    // 初始值(初始状态，初始化)
                    @Override
                    public Integer createAccumulator() {
                        return 0;
                    }

                    /**
                     * 叠加、聚合，每来一条数据调用一次
                     * @param sensorReading 新来的数据
                     * @param value(accumulator) 当前的状态
                     * @return 返回当前状态的类型
                     */
                    @Override
                    public Integer add(SensorReading sensorReading, Integer value) {
                        return value + 1;
                    }

                    /**
                     * 输出结果，窗口关闭时触发计算的输出结果，触发一次窗口计算就调用一次
                     * @param value(accumulator ACC类型)
                     * @return 返回输出 OUT类型
                     */
                    @Override
                    public Integer getResult(Integer value) {
                        return value;
                    }

                    /**
                     * 合并两个累加器，两个窗口合并时触发的计算，一般在会话窗口中才会涉及
                     * @param value
                     * @param acc
                     * @return
                     */
                    @Override
                    public Integer merge(Integer value, Integer acc) {
                        return acc + 1;
                    }
                });
        outputStream.print();
        env.execute();
    }
}
