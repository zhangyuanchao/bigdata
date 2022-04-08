package com.mnemosyne.hadoop.mr.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {
    // hadoop框架中, 它是一个分布式的, 数据不可避免地要做序列化和反序列化
    // hadoop有自己地一套序列化、反序列化类型
    // 或者可以开发自己地序列化、反序列化类型, 但必须实现它的接口, 实现比较器接口
    // 排序--->比较

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    /**
     * 输入格式时TextInputFormat
     * @param key 是每一行字符串第一个字节面向源文件的偏移量
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // StringTokenizer itr = new StringTokenizer(value.toString());
        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
            word.set(itr.nextToken());
            context.write(word, one);
        }
    }
}
