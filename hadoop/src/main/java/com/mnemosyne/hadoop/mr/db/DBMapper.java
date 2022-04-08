package com.mnemosyne.hadoop.mr.db;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DBMapper extends Mapper<LongWritable, HistoryBalance, Text, HistoryBalance> {

    @Override
    protected void map(LongWritable key, HistoryBalance value, Context context) throws IOException, InterruptedException {
        context.write(new Text(value.getBalanceDate()), value);
    }
}
