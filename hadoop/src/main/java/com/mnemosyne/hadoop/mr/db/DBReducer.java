package com.mnemosyne.hadoop.mr.db;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.math.BigDecimal;

public class DBReducer extends Reducer<Text, HistoryBalance, Text, DoubleWritable> {

    private DoubleWritable result = new DoubleWritable();

    @Override
    protected void reduce(Text key, Iterable<HistoryBalance> values, Context context) throws IOException, InterruptedException {
        BigDecimal total = new BigDecimal(0);
        for(HistoryBalance balance : values) {
            total.add(balance.getBalance());
        }
        result.set(total.doubleValue());
        context.write(key, result);
    }
}
