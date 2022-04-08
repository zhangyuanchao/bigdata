package com.themis.flink.window;

import com.themis.flink.model.SensorReading;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

/**
 * 其他可选api, 例如处理延迟数据
 */
public class TimeWindowLate {

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

        // OutputTag<SensorReading> outputTag = new OutputTag<SensorReading>("id");
        OutputTag<SensorReading> outputTag = new OutputTag<SensorReading>("id"){};
        SingleOutputStreamOperator<SensorReading> sumStream = dataStream
                .keyBy(new KeySelector<SensorReading, String>() {
                    @Override
                    public String getKey(SensorReading sensorReading) throws Exception {
                        return null;
                    }
                })
                .window(TumblingProcessingTimeWindows.of(Time.seconds(15)))
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(outputTag)
                .sum("temperature");
        sumStream.getSideOutput(outputTag).print("late");
        env.execute();
    }
}
