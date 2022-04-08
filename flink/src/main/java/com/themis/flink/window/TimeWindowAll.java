package com.themis.flink.window;

import com.themis.flink.model.SensorReading;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 时间窗口:全窗口函数
 */
public class TimeWindowAll {

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

        DataStream<Tuple3<String, Long, Integer>> outputStream = dataStream
                .keyBy(new KeySelector<SensorReading, String>() {
                    @Override
                    public String getKey(SensorReading sensorReading) throws Exception {
                        return sensorReading.getId();
                    }
                })
                .window(TumblingProcessingTimeWindows.of(Time.seconds(15)))
                .apply(new WindowFunction<SensorReading, Tuple3<String, Long, Integer>, String, TimeWindow>() {
                    @Override
                    public void apply(String id, TimeWindow timeWindow, Iterable<SensorReading> iterable, Collector<Tuple3<String, Long, Integer>> out) throws Exception {
                        //  窗口结束时间
                        Long end = timeWindow.getEnd();
                        Integer count = IteratorUtils.toList(iterable.iterator()).size();
                        out.collect(new Tuple3<>(id, end, count));
                    }
                });
        outputStream.print();
        env.execute();
    }
}
