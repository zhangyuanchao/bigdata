package com.themis.flink.transformation;

import com.themis.flink.model.SensorReading;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

public class TransformationConnect {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> inputStream = env.readTextFile("C:\\study\\workspace\\java\\bd\\bigdata\\flink\\src\\main\\resources\\sensor.txt");
        DataStream<SensorReading> sensorStream = inputStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String line) throws Exception {
                String[] fields = line.split(",");
                return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
            }
        });

        DataStream<SensorReading> highTempStream = sensorStream.filter(new FilterFunction<SensorReading>() {
            @Override
            public boolean filter(SensorReading sensorReading) throws Exception {
                return sensorReading.getTemperature() >= 35;
            }
        });

        DataStream<SensorReading> lowTempStream = sensorStream.filter(new FilterFunction<SensorReading>() {
            @Override
            public boolean filter(SensorReading sensorReading) throws Exception {
                return sensorReading.getTemperature() < 35;
            }
        });

        DataStream<Tuple2<String, Double>> warningStream = highTempStream.map(new MapFunction<SensorReading, Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> map(SensorReading sensorReading) throws Exception {
                return new Tuple2<>(sensorReading.getId(), sensorReading.getTemperature());
            }
        });

        ConnectedStreams<Tuple2<String, Double>, SensorReading> connectedStreams = warningStream.connect(lowTempStream);
        DataStream<Object> outputStream = connectedStreams.map(new CoMapFunction<Tuple2<String, Double>, SensorReading, Object>() {
            @Override
            public Object map1(Tuple2<String, Double> value) throws Exception {
                return new Tuple3<>(value.f0, value.f1, "warning");
            }

            @Override
            public Object map2(SensorReading sensorReading) throws Exception {
                return new Tuple2<>(sensorReading.getId(), "healthy");
            }
        });
        outputStream.print();
        env.execute();
    }
}
