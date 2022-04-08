package com.themis.flink.connector;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.themis.flink.model.SensorReading;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class ConnectorMysqlSource {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<SensorReading> inputStream = env.addSource(new JdbcReader());

        inputStream.print("from mysql");
        env.execute();
    }

    public static class JdbcReader extends RichSourceFunction<SensorReading> {

        private DruidDataSource dataSource;
        private boolean running = true;

        @Override
        public void open(Configuration parameters) throws Exception {
            dataSource = new DruidDataSource();
            dataSource.setDriverClassName("com.mysql.jdbc.Driver");
            dataSource.setUrl("jdbc:mysql://cdb1te.test.srv.mc.dd:3306/fms");
            dataSource.setUsername("root");
            dataSource.setPassword("1234qwer");
            dataSource.setInitialSize(10);
            dataSource.setMinIdle(10);
            dataSource.setMaxActive(20);
            dataSource.setMaxWait(1000 * 20);
            dataSource.setTimeBetweenEvictionRunsMillis(1000 * 60);
            dataSource.setMaxEvictableIdleTimeMillis(1000 * 60 * 60 * 10);
            dataSource.setMinEvictableIdleTimeMillis(1000 * 60 * 60 * 10);
            dataSource.setTestWhileIdle(true);
            dataSource.setTestOnBorrow(true);
            dataSource.setTestOnReturn(false);
            dataSource.setValidationQuery("select 1");
        }

        @Override
        public void run(SourceContext<SensorReading> context) throws Exception {
            DruidPooledConnection connection = dataSource.getConnection();
            PreparedStatement ps = connection.prepareStatement("select id, time_stamp, temperature from md_test1 where id like ?;");
            ps.setString(1, "sensor_1%");
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                SensorReading sensorReading = new SensorReading();
                sensorReading.setId(rs.getString("id"));
                sensorReading.setTimestamp(rs.getLong("time_stamp"));
                sensorReading.setTemperature(rs.getDouble("temperature"));
                context.collect(sensorReading);
            }
        }

        @Override
        public void cancel() {
            try {
                super.close();
                if (dataSource != null) {
                    dataSource.close();
                }
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
            running = false;
        }
    }
}
