package com.zhq.bigdata.hadoop.mr.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class WordCountApp {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {

        System.setProperty("HADOOP_USER_NAME","hadoop");

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://hadoop000:8020");
        conf.set("dfs.client.use.datanode.hostname", "true");

        Job job = Job.getInstance(conf);
        job.setJarByClass(WordCountApp.class);
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop000:8020"),conf, "hadoop");
        Path outputPath = new Path("/wordcount/output");
        if(fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath,true);
        }

        Path inputPath = new Path("/wordcount/input");

        FileInputFormat.setInputPaths(job,inputPath);
        FileOutputFormat.setOutputPath(job,outputPath);

        boolean result = job.waitForCompletion(true);
        System.exit(result?0:-1);
    }
}
