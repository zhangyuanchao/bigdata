package com.themis.flink.connector;

import com.alibaba.druid.pool.DruidDataSource;
import com.themis.flink.model.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;

public class ConnectorMysqlSink {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        DataStream<String> inputStream = env.readTextFile("C:\\study\\workspace\\java\\bd\\bigdata\\flink\\src\\main\\resources\\sensor.txt");
        DataStream<SensorReading> sensorStream = inputStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String line) throws Exception {
                String[] fields = line.split(",");
                return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
            }
        });
        sensorStream.addSink(new JdbcWriter());
        env.execute();
    }

    public static class JdbcWriter extends RichSinkFunction<SensorReading> {
        private DruidDataSource dataSource;

        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("init.....");
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
        public void close() throws Exception {
            System.out.println("close.....");
            super.close();
            //关闭连接和释放资源
            try {
                super.close();
                if (dataSource != null) {
                    dataSource.close();
                }
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        }

        /**
         * 每条数据的插入都要调用一次 invoke() 方法
         *
         * @param value
         * @param context
         * @throws Exception
         */
        @Override
        public void invoke(SensorReading value, Context context) throws Exception {
            Connection connection = dataSource.getConnection();
            PreparedStatement ps = connection.prepareStatement("insert into md_test1(id, time_stamp, temperature) values (?, ?, ?)");
            if (ps == null) {
                return;
            }
            //遍历数据集合
            //for (Student student : value) {
            ps.setString(1, value.getId());
            ps.setLong(2, value.getTimestamp());
            ps.setDouble(3, value.getTemperature());
            ps.addBatch();
            //}
            int[] count = ps.executeBatch();//批量后执行
            System.out.println("成功了插入了" + count.length + "行数据");
        }
    }
}
