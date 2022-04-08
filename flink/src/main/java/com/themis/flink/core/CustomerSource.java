package com.themis.flink.core;

import com.themis.flink.model.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import sun.management.Sensor;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * 自定义流输入源
 */
public class CustomerSource {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<SensorReading> dataStream = env.addSource(new SensorSource());
        dataStream.print();
        env.execute();
    }

    public static class SensorSource implements SourceFunction<SensorReading> {

        private boolean running = true;

        @Override
        public void run(SourceContext<SensorReading> context) throws Exception {
            Map<String, Double> sensorMap = new HashMap<>();
            Random random = new Random();
            for (int i = 1; i <= 10; i++) {
                sensorMap.put("sensor_" + i, 60 + random.nextGaussian() * 20);
            }
            while (running) {
                for (String key : sensorMap.keySet()) {
                    context.collect(new SensorReading(key, System.currentTimeMillis(), sensorMap.get(key) + random.nextGaussian()));
                }
                Thread.sleep(500);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
