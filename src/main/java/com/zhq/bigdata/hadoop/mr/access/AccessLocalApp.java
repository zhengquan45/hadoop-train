package com.zhq.bigdata.hadoop.mr.access;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class AccessLocalApp {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setJarByClass(AccessLocalApp.class);
        job.setMapperClass(AccessMapper.class);
        job.setReducerClass(AccessReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Access.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Access.class);

        job.setPartitionerClass(AccessPartitioner.class);
        job.setNumReduceTasks(3);

        Path inputPath = new Path("access/input");
        Path outputPath = new Path("access/output");

        FileInputFormat.setInputPaths(job,inputPath);
        FileOutputFormat.setOutputPath(job,outputPath);

        job.waitForCompletion(true);
    }
}
