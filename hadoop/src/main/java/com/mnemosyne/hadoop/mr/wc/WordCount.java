package com.mnemosyne.hadoop.mr.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordCount {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration(true);

        // 让框架知道是windows异构平台运行
        conf.set("mapreduce.app-submission.cross-platform", "true");
        // Create a new Job
        Job job = Job.getInstance(conf);
        job.setJarByClass(WordCount.class);
        job.setJar("C:\\study\\workspace\\java\\hadoop\\target\\hadoop-1.0.0-SNAPSHOT.jar");

        // Specify various job-specific parameters
        job.setJobName("wordcount");

        Path infile = new Path("/data/wc/input");
        TextInputFormat.addInputPath(job, infile);

        Path outfile = new Path("/data/wc/output");
        TextOutputFormat.setOutputPath(job, outfile);

        job.setMapperClass(WordCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setReducerClass(WordCountReducer.class);

        // Submit the job, then poll for progress until the job is complete
        job.waitForCompletion(true);
    }
}
