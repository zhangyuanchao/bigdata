package com.themis.flink.connector;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class ConnectorKafkaSource {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "192.168.48.161:9092");
        props.setProperty("zookeeper.connect", "192.168.48.161:2181,192.168.48.162:2181,192.168.48.163:2181");
        props.setProperty("group.id", "flink-group");
        FlinkKafkaConsumer consumer = new FlinkKafkaConsumer<String>("apple", new SimpleStringSchema(), props);
        DataStream<String> inputStream = env.addSource(consumer);

        inputStream.print("from kafka");
        env.execute();
    }
}
