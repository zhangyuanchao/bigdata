package com.mnemosyne.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;

public class HdfsOperate {

    private static final Logger logger = LoggerFactory.getLogger(HdfsOperate.class);

    private static Configuration conf = null;
    private static FileSystem fs = null;

    static {
        conf = new Configuration(true); // true会加载resources目录下的配置文件
        try {
            // fs = FileSystem.get(conf); // 默认读取环境变量HADOOP_USER_NAME的用户
            fs = FileSystem.get(URI.create("hdfs://nn-services/"), conf, "root");
            // 返回的fs类型参考schema
//            <property>
//            <name>fs.defaultFS</name>
//            <value>hdfs://nn-services</value>
//            </property>
        } catch (Exception e) {
            logger.error("连接namenode失败:{}", e.getMessage());
        }
    }

    public static void main(String[] args) throws Exception {
        // mkdir();
        // uploadFile();
        blocks();
        fs.close();
    }

    public static void mkdir() throws Exception {
        Path dir = new Path("/user/root/product");
        if (fs.exists(dir)) {
            fs.delete(dir, true);
        }
        fs.mkdirs(dir);
    }

    public static void uploadFile() throws Exception {
        BufferedInputStream os = new BufferedInputStream(new FileInputStream(new File("C:\\study\\DORRIT.txt")));
        Path outfile = new Path("/user/root/product/rit.txt");
        FSDataOutputStream fis = fs.create(outfile);
        IOUtils.copyBytes(os, fis, conf, true);
    }

    public static void blocks() throws Exception {
        Path file = new Path("/user/root/data.txt");
        FileStatus fileStatus = fs.getFileStatus(file); // 获取文件元数据
        BlockLocation[] blocks = fs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
        // BlockLocation[] blocks = fs.getFileBlockLocations(file, 0, 1000);
        for (BlockLocation block : blocks) {
            System.out.println(block);
        }
        FSDataInputStream is = fs.open(file);
    }
}
