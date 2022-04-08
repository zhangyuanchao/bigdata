package com.themis.flink.transformation;

import com.themis.flink.model.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformationReduce {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> inputStream = env.readTextFile("C:\\study\\workspace\\java\\bd\\bigdata\\flink\\src\\main\\resources\\sensor.txt");
        DataStream<SensorReading> sensorStream = inputStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String s) throws Exception {
                String[] fields = s.split(",");
                return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
            }
        });

        KeyedStream<SensorReading, String> keyedStream = sensorStream.keyBy(new KeySelector<SensorReading, String>() {

            @Override
            public String getKey(SensorReading sensorReading) throws Exception {
                return sensorReading.getId();
            }
        });


        env.execute();
    }
}
