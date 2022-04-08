package com.mnemosyne.hadoop.mr.db;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MapReduceDB {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration(true);
        conf.set("mapreduce.app-submission.cross-platform", "true");
        DBConfiguration.configureDB(conf, "com.mysql.jdbc.Driver", "jdbc:mysql://node4:3306/poseidon?useUnicode=true&characterEncoding=utf8&allowMultiQueries=true&zeroDateTimeBehavior=convertToNull&useSSL=false",
                "root", "hadoop");

        Job job = Job.getInstance(conf);

        job.setJarByClass(MapReduceDB.class);
        job.setMapperClass(DBMapper.class);
        job.setReducerClass(DBReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(HistoryBalance.class);

        String[] fields = {"id", "account_name", "account_no", "account_type", "corporate_company_name", "corporate_company_id",
                "use_company_name", "use_company_id", "cooperate_bank_name", "cooperate_bank_id", "account_sub_bank_name",
                "currency", "balance", "available_balance", "frozen_balance", "balance_date", "update_time", "create_time"};
        DBInputFormat.setInput(job, HistoryBalance.class, "history_balance", null, null, fields);

        Path path = new Path("/db/output");
        //输出路径设置
        FileOutputFormat.setOutputPath(job, path);

        job.waitForCompletion(true);
    }
}
