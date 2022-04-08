package com.themis.flink.connector;

import com.themis.flink.model.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

public class ConnectorKafkaSink {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> inputStream = env.readTextFile("C:\\study\\workspace\\java\\bd\\bigdata\\flink\\src\\main\\resources\\sensor.txt");
//        DataStream<SensorReading> sensorStream = inputStream.map(new MapFunction<String, SensorReading>() {
//            @Override
//            public SensorReading map(String line) throws Exception {
//                String[] fields = line.split(",");
//                return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
//            }
//        });

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "192.168.48.161:9092");
        props.setProperty("zookeeper.connect", "192.168.48.161:2181,192.168.48.162:2181,192.168.48.163:2181");
        props.setProperty("group.id", "flink-group");
        FlinkKafkaProducer producer = new FlinkKafkaProducer("apple", new SimpleStringSchema(), props);
        inputStream.addSink(producer);
        env.execute();
    }
}
